"""Actual process death between a committed external effect and queue acknowledgement."""

import json
import multiprocessing as mp
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

from durableq import Queue


def effect(path, job, deduplicate):
    db = sqlite3.connect(path)
    try:
        if deduplicate:
            db.execute('INSERT OR IGNORE INTO effects(job_id) VALUES (?)', (job,))
        else:
            db.execute('INSERT INTO effects(job_id) VALUES (?)', (job,))
        db.commit()
    finally:
        db.close()


def doomed_worker(path, effect_path, deduplicate, ready):
    with Queue(path) as queue:
        claim = queue.claim(lease=30)
        effect(effect_path, claim.id, deduplicate)
        ready.send({'id': claim.id, 'deadline': claim.deadline})
        ready.close()
        os._exit(23)  # Bypass cleanup; do not acknowledge the queue job.


def scenario(deduplicate):
    ctx = mp.get_context('spawn')
    with tempfile.TemporaryDirectory(prefix='durableq-fault-') as temp:
        path, effect_path = Path(temp) / 'queue.db', Path(temp) / 'effects.db'
        Queue.initialize(path)
        with closing(sqlite3.connect(effect_path)) as db:
            unique = ' UNIQUE' if deduplicate else ''
            db.execute(f'CREATE TABLE effects(job_id TEXT NOT NULL{unique})')
            db.commit()
        with Queue(path) as queue:
            job = queue.enqueue('external_effect', {})
        receive, send = ctx.Pipe(duplex=False)
        child = ctx.Process(target=doomed_worker, args=(str(path), str(effect_path), deduplicate, send))
        child.start()
        send.close()
        try:
            if not receive.poll(20):
                raise TimeoutError('fault worker failed to reach effect boundary')
            message = receive.recv()
            child.join(20)
            if child.exitcode != 23:
                raise RuntimeError(f'Expected injected crash, got {child.exitcode}')
            # Controlled clock advances expiration without a 30-second sleep.
            with Queue(path, clock=lambda: message['deadline'] + 1) as queue:
                retry = queue.claim()
                effect(effect_path, retry.id, deduplicate)
                queue.ack(retry)
                state = queue.get(job)
            with closing(sqlite3.connect(effect_path)) as db:
                effects = db.execute('SELECT count(*) FROM effects').fetchone()[0]
            expected = 1 if deduplicate else 2
            if effects != expected or state['attempts'] != 2 or state['state'] != 'done':
                raise AssertionError('Unexpected fault experiment result')
            return dict(external_deduplication=deduplicate, process_exit_code=child.exitcode,
                        executions=state['attempts'], side_effects=effects, final_state=state['state'])
        finally:
            receive.close()
            if child.is_alive():
                child.terminate()
                child.join()


def main():
    output = {'fault': 'process death after effect commit, before ack',
              'clock': 'controlled advance past persisted lease deadline',
              'scenarios': [scenario(False), scenario(True)]}
    path = Path('results')
    path.mkdir(exist_ok=True)
    (path / 'faults.json').write_text(json.dumps(output, indent=2), encoding='utf-8')
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
