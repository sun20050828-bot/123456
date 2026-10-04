import multiprocessing as mp
import os
import random
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from durableq import Conflict, LeaseLost, Queue
from durableq.store import wal_is_patched
from durableq.worker import run_once


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def drain(path, output):
    ids = []
    try:
        # This test measures safety, not an IO deadline. Hosted Windows runners
        # can spend >10s syncing an 80-job backlog; allow a separate test budget.
        with Queue(path, timeout=60) as queue:
            while (claim := queue.claim(lease=60)) is not None:
                queue.ack(claim, claim.payload)
                ids.append(claim.id)
        output.put(('ok', ids))
    except BaseException as error:
        output.put(('error', repr(error)))


def same_key(path, output):
    try:
        with Queue(path) as queue:
            output.put(queue.enqueue('echo', {'x': 1}, key='shared'))
    except BaseException as error:
        output.put(repr(error))


def crash_after_claim(path, output):
    with Queue(path) as queue:
        claim = queue.claim(lease=0.01)
        output.send(claim.id)
        output.close()
        os._exit(17)


def crash_uncommitted_ack(path, output):
    with Queue(path) as queue:
        claim = queue.claim(lease=1)
        queue.db.execute('BEGIN IMMEDIATE')
        queue.db.execute("UPDATE jobs SET state='done',token=NULL,deadline=NULL WHERE id=?", (claim.id,))
        output.send(claim.id)
        output.close()
        os._exit(18)


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'queue.db'
        Queue.initialize(self.path)
        self.clock = Clock()
        self.queue = Queue(self.path, clock=self.clock)

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def job(self, **kw):
        return self.queue.enqueue('echo', {'hello': 'world'}, **kw)

    def test_roundtrip_and_audit(self):
        job = self.job()
        claim = self.queue.claim()
        self.assertEqual(claim.id, job)
        self.queue.ack(claim, [1, 2])
        self.assertEqual(self.queue.get(job)['result'], [1, 2])
        self.assertEqual([e['kind'] for e in self.queue.events(job)], ['enqueued', 'claimed', 'done'])
        self.assertEqual(self.queue.stats(), dict(ready=0, running=0, done=1, dead=0))

    def test_deduplication_includes_terminal_jobs(self):
        job = self.job(key='same')
        self.queue.ack(self.queue.claim())
        self.assertEqual(self.job(key='same'), job)
        self.assertEqual(len(self.queue.events(job)), 3)

    def test_key_conflict_and_rollback(self):
        job = self.job(key='same')
        with self.assertRaises(Conflict):
            self.queue.enqueue('echo', {'different': True}, key='same')
        with self.assertRaises(Conflict):
            self.job(key='same', priority=3)
        self.assertEqual(self.queue.stats()['ready'], 1)
        self.assertEqual(self.queue.get(job)['priority'], 0)

    def test_json_key_order_is_canonical(self):
        first = self.queue.enqueue('echo', {'a': 1, 'b': 2}, key='dict')
        self.assertEqual(first, self.queue.enqueue('echo', {'b': 2, 'a': 1}, key='dict'))

    def test_priority_and_delay(self):
        self.job(delay=5, priority=99)
        low = self.job(priority=-1)
        high = self.job(priority=2)
        self.assertEqual(self.queue.claim().id, high)
        self.assertEqual(self.queue.claim().id, low)
        self.assertIsNone(self.queue.claim())

    def test_delay_boundary(self):
        job = self.job(delay=5)
        self.assertIsNone(self.queue.claim())
        self.clock.now += 5
        self.assertEqual(self.queue.claim().id, job)

    def test_expired_token_cannot_ack_even_before_recovery(self):
        self.job()
        claim = self.queue.claim(lease=2)
        self.clock.now += 2
        with self.assertRaises(LeaseLost):
            self.queue.ack(claim)
        with self.assertRaises(LeaseLost):
            self.queue.renew(claim)
        with self.assertRaises(LeaseLost):
            self.queue.fail(claim, 'old')

    def test_fencing_rejects_old_generation(self):
        self.job()
        old = self.queue.claim(lease=2)
        self.clock.now += 2
        new = self.queue.claim()
        self.assertEqual(new.generation, 2)
        self.assertNotEqual(new.token, old.token)
        with self.assertRaises(LeaseLost):
            self.queue.ack(old)
        self.queue.ack(new)

    def test_renewal_does_not_shorten_lease(self):
        self.job()
        claim = self.queue.claim(lease=30)
        self.assertEqual(self.queue.renew(claim, lease=1), 1030)
        self.clock.now += 29
        self.assertEqual(self.queue.renew(claim, lease=30), 1059)
        self.clock.now += 2
        self.queue.ack(claim)  # Claim dataclass deadline is informational, DB is authoritative.

    def test_crashes_consume_attempt_budget(self):
        job = self.job(max_attempts=2)
        self.queue.claim(lease=1)
        self.clock.now += 1
        self.assertEqual(self.queue.claim(lease=1).attempt, 2)
        self.clock.now += 1
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.get(job)['state'], 'dead')

    def test_retry_exponential_backoff_and_dead_letter(self):
        job = self.job(max_attempts=3)
        for delay in (1, 2):
            claim = self.queue.claim()
            self.assertEqual(self.queue.fail(claim, 'transient'), 'ready')
            self.assertIsNone(self.queue.claim())
            self.clock.now += delay
        self.assertEqual(self.queue.fail(self.queue.claim(), 'fatal'), 'dead')
        self.assertEqual(self.queue.get(job)['attempts'], 3)

    def test_backoff_cap(self):
        job = self.job()
        self.queue.fail(self.queue.claim(), 'oops', base_delay=100, cap=5)
        self.assertEqual(self.queue.get(job)['available'], 1005)

    def test_double_ack_rejected(self):
        self.job()
        claim = self.queue.claim()
        self.queue.ack(claim)
        with self.assertRaises(LeaseLost):
            self.queue.ack(claim)

    def test_invalid_json_cannot_commit(self):
        with self.assertRaises(ValueError):
            self.queue.enqueue('echo', float('nan'))
        self.assertEqual(sum(self.queue.stats().values()), 0)
        self.job()
        claim = self.queue.claim()
        with self.assertRaises(ValueError):
            self.queue.ack(claim, float('inf'))
        self.assertEqual(self.queue.stats()['running'], 1)

    def test_invalid_lease(self):
        for lease in (0, -1, float('nan'), float('inf'), True):
            with self.assertRaises(ValueError):
                self.queue.claim(lease=lease)

    def test_invalid_enqueue_options(self):
        for opts in ({'delay': -1}, {'delay': float('nan')}, {'max_attempts': 0},
                     {'max_attempts': True}, {'priority': 1.5}, {'key': ''}):
            with self.assertRaises(ValueError):
                self.job(**opts)

    def test_clock_sampled_in_transaction(self):
        def checked_clock():
            self.assertTrue(self.queue.db.in_transaction)
            return 1234
        self.queue.clock = checked_clock
        self.job()
        self.assertEqual(self.queue.claim(lease=10).deadline, 1244)

    def test_reopen_preserves_result(self):
        job = self.job()
        self.queue.ack(self.queue.claim(), {'saved': True})
        with Queue(self.path) as other:
            self.assertEqual(other.get(job)['result'], {'saved': True})

    def test_unknown_job(self):
        self.assertIsNone(self.queue.get('not-present'))

    def test_worker_handler_failure_is_retried(self):
        self.job()
        def bad(claim):
            raise ValueError('handler failure')
        self.assertEqual(run_once(self.queue, {'echo': bad}), 'failed')
        self.assertEqual(self.queue.stats()['ready'], 1)

    def test_worker_idle_and_success(self):
        self.assertEqual(run_once(self.queue, {}), 'idle')
        self.job()
        self.assertEqual(run_once(self.queue, {'echo': lambda c: c.payload}), 'done')

    def test_ack_storage_errors_are_not_handler_errors(self):
        self.job()
        with patch.object(self.queue, 'ack', side_effect=sqlite3.OperationalError('disk full')):
            with self.assertRaises(sqlite3.OperationalError):
                run_once(self.queue, {'echo': lambda c: 1})
        self.assertEqual(self.queue.stats()['running'], 1)

    def test_transaction_and_event_rollback_together(self):
        with patch.object(self.queue, '_event', side_effect=RuntimeError('injected fault')):
            with self.assertRaises(RuntimeError):
                self.job()
        self.assertEqual(sum(self.queue.stats().values()), 0)

    def test_foreign_key_and_state_constraints(self):
        job = self.job()
        with self.assertRaises(sqlite3.IntegrityError):
            self.queue.db.execute("UPDATE jobs SET state='running' WHERE id=?", (job,))
        with self.assertRaises(sqlite3.IntegrityError):
            self.queue.db.execute("INSERT INTO events(job_id,kind,generation,at) VALUES ('missing','done',1,0)")

    def test_wal_runtime_gate(self):
        for version in ((3, 51, 3), (3, 52, 0), (3, 50, 7), (3, 44, 6)):
            self.assertTrue(wal_is_patched(version))
        for version in ((3, 50, 4), (3, 51, 2), (3, 45, 0), (3, 49, 9)):
            self.assertFalse(wal_is_patched(version))
        with patch('sqlite3.sqlite_version_info', (3, 50, 4)):
            with self.assertRaises(RuntimeError):
                Queue.initialize(Path(self.temp.name) / 'wal.db', journal='WAL')

    def test_spawn_concurrent_consumers_no_duplicate_commits(self):
        expected = {self.job() for _ in range(80)}
        ctx = mp.get_context('spawn')
        output = ctx.Queue()
        workers = [ctx.Process(target=drain, args=(str(self.path), output)) for _ in range(4)]
        try:
            for worker in workers:
                worker.start()
            results = [output.get(timeout=90) for _ in workers]
            for worker in workers:
                worker.join(30)
                self.assertEqual(worker.exitcode, 0)
            self.assertTrue(all(status == 'ok' for status, _ in results), results)
            actual = [job for _, jobs in results for job in jobs]
            self.assertEqual(len(actual), len(set(actual)))
            self.assertEqual(set(actual), expected)
            self.assertEqual(self.queue.stats()['done'], 80)
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join()
            output.close()
            output.join_thread()

    def test_spawn_concurrent_producers_same_key(self):
        ctx = mp.get_context('spawn')
        output = ctx.Queue()
        workers = [ctx.Process(target=same_key, args=(str(self.path), output)) for _ in range(4)]
        try:
            for worker in workers:
                worker.start()
            ids = [output.get(timeout=30) for _ in workers]
            for worker in workers:
                worker.join(30)
                self.assertEqual(worker.exitcode, 0)
            self.assertEqual(len(set(ids)), 1)
            self.assertIsNotNone(self.queue.get(ids[0]))
            self.assertEqual(self.queue.stats()['ready'], 1)
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join()
            output.close()
            output.join_thread()

    def test_process_crash_after_claim_recovers(self):
        job = self.job()
        ctx = mp.get_context('spawn')
        receive, send = ctx.Pipe(duplex=False)
        worker = ctx.Process(target=crash_after_claim, args=(str(self.path), send))
        worker.start()
        send.close()
        try:
            self.assertTrue(receive.poll(15))
            self.assertEqual(receive.recv(), job)
            worker.join(15)
            self.assertEqual(worker.exitcode, 17)
            self.assertEqual(self.queue.get(job)['state'], 'running')
            self.clock.now = self.queue.get(job)['deadline'] + 1
            claim = self.queue.claim()
            self.assertEqual(claim.attempt, 2)
            self.queue.ack(claim)
        finally:
            receive.close()
            if worker.is_alive():
                worker.terminate()
                worker.join()

    def test_process_crash_during_ack_rolls_back(self):
        job = self.job()
        ctx = mp.get_context('spawn')
        receive, send = ctx.Pipe(duplex=False)
        worker = ctx.Process(target=crash_uncommitted_ack, args=(str(self.path), send))
        worker.start()
        send.close()
        try:
            self.assertTrue(receive.poll(15))
            self.assertEqual(receive.recv(), job)
            worker.join(15)
            self.assertEqual(worker.exitcode, 18)
            self.assertEqual(self.queue.get(job)['state'], 'running')
            self.assertEqual([e['kind'] for e in self.queue.events(job)], ['enqueued', 'claimed'])
            self.clock.now = self.queue.get(job)['deadline'] + 1
            self.queue.ack(self.queue.claim())
            self.assertEqual(self.queue.get(job)['state'], 'done')
        finally:
            receive.close()
            if worker.is_alive():
                worker.terminate()
                worker.join()

    def test_randomized_history_invariants(self):
        rng = random.Random(2026)
        jobs = [self.job(max_attempts=4) for _ in range(20)]
        held = []
        for _ in range(400):
            self.clock.now += rng.choice([0, 0.5, 1])
            action = rng.choice(['claim', 'ack', 'fail', 'renew', 'recover'])
            if action == 'claim':
                claim = self.queue.claim(lease=3)
                if claim:
                    held.append(claim)
            elif action == 'recover':
                self.queue.recover()
            elif held:
                claim = rng.choice(held)
                try:
                    if action == 'ack':
                        self.queue.ack(claim)
                    elif action == 'fail':
                        self.queue.fail(claim, 'random fault')
                    else:
                        self.queue.renew(claim, lease=3)
                except LeaseLost:
                    pass
            self.assertEqual(sum(self.queue.stats().values()), 20)
            for job in jobs:
                row = self.queue.get(job)
                self.assertLessEqual(row['attempts'], row['max_attempts'])
                self.assertEqual(row['generation'], row['attempts'])
                self.assertEqual(row['state'] == 'running', row['token'] is not None)
        for job in jobs:
            events = self.queue.events(job)
            claims = [e['generation'] for e in events if e['kind'] == 'claimed']
            self.assertEqual(claims, list(range(1, len(claims) + 1)))
            self.assertLessEqual(sum(e['kind'] == 'done' for e in events), 1)


@unittest.skipUnless(wal_is_patched(sqlite3.sqlite_version_info), 'WAL requires patched SQLite')
class WALQueueTests(QueueTests):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'queue.db'
        Queue.initialize(self.path, journal='WAL')
        self.clock = Clock()
        self.queue = Queue(self.path, clock=self.clock)


if __name__ == '__main__':
    unittest.main()
