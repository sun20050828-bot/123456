"""Serialize state transitions, never handler execution, with BEGIN IMMEDIATE."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator


class LeaseLost(RuntimeError):
    """The token is no longer the current, unexpired owner of the job."""


class Conflict(ValueError):
    """An idempotency key was reused with a different request."""


@dataclass(frozen=True)
class Claim:
    id: str
    task: str
    payload: Any
    token: str
    generation: int
    attempt: int
    deadline: float


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, idem_key TEXT UNIQUE, request TEXT NOT NULL,
    task TEXT NOT NULL, payload TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('ready','running','done','dead')),
    priority INTEGER NOT NULL, available REAL NOT NULL, created REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    generation INTEGER NOT NULL DEFAULT 0, token TEXT, deadline REAL,
    result TEXT, error TEXT,
    CHECK ((state = 'running' AND token IS NOT NULL AND deadline IS NOT NULL)
        OR (state <> 'running' AND token IS NULL AND deadline IS NULL))
);
CREATE INDEX IF NOT EXISTS ready_jobs ON jobs(priority DESC, available, created, id)
    WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS expired_jobs ON jobs(deadline) WHERE state = 'running';
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
    kind TEXT NOT NULL, generation INTEGER NOT NULL, at REAL NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(id)
);
CREATE INDEX IF NOT EXISTS job_events ON events(job_id, seq);
"""


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def positive(value: float, name: str) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be finite and positive')
    return value


def wal_is_patched(version: tuple[int, ...]) -> bool:
    # SQLite's official WAL-reset fix: mainline 3.51.3, backports 3.50.7/3.44.6.
    return (version >= (3, 51, 3) or
            ((3, 50, 7) <= version < (3, 51, 0)) or
            ((3, 44, 6) <= version < (3, 45, 0)))


class Queue:
    """One instance per process/thread; each instance owns one SQLite connection.

    Call initialize once before spawning workers. Constructor opens an existing
    database and never changes its journal mode while other workers are active.
    Persisted times use the same host's wall clock, sampled AFTER lock acquisition.
    """

    @classmethod
    def initialize(cls, path: str | Path, *, journal: str = 'DELETE') -> None:
        journal = journal.upper()
        if journal not in {'DELETE', 'WAL'}:
            raise ValueError('journal must be DELETE or WAL')
        if journal == 'WAL' and not wal_is_patched(sqlite3.sqlite_version_info):
            raise RuntimeError('WAL requires patched SQLite >=3.51.3, 3.50.7 or 3.44.6')
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(path, timeout=10, isolation_level=None)
        try:
            actual = db.execute(f'PRAGMA journal_mode={journal}').fetchone()[0]
            if actual.upper() != journal:
                raise RuntimeError(f'Cannot enable {journal}; got {actual}')
            db.execute('PRAGMA synchronous=FULL')
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1):
                raise RuntimeError(f'Unsupported schema version: {version}')
            db.executescript('BEGIN IMMEDIATE;\n' + SCHEMA + '\nPRAGMA user_version=1; COMMIT;')
        finally:
            db.close()

    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time,
                 timeout: float = 10, synchronous: str = 'FULL'):
        positive(timeout, 'timeout')
        synchronous = synchronous.upper()
        if synchronous not in {'FULL', 'NORMAL'}:
            raise ValueError('synchronous must be FULL or NORMAL')
        # mode=rw prevents a typo from silently creating an uninitialized database.
        uri = Path(path).resolve().as_uri() + '?mode=rw'
        self.db = sqlite3.connect(uri, uri=True, timeout=timeout, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            if self.db.execute('PRAGMA user_version').fetchone()[0] != 1:
                raise RuntimeError('Initialize the database first')
            journal = self.db.execute('PRAGMA journal_mode').fetchone()[0].upper()
            if journal == 'WAL' and not wal_is_patched(sqlite3.sqlite_version_info):
                raise RuntimeError('Existing WAL database needs a patched SQLite runtime')
            self.db.execute(f'PRAGMA synchronous={synchronous}')
            self.db.execute('PRAGMA foreign_keys=ON')
            self.clock = clock
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> Queue:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @contextmanager
    def _tx(self) -> Iterator[float]:
        self.db.execute('BEGIN IMMEDIATE')
        try:
            now = self.clock()  # Do not let lock waiting consume a new lease.
            if not math.isfinite(now):
                raise ValueError('clock must return a finite timestamp')
            yield now
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _event(self, job: str, kind: str, generation: int, now: float) -> None:
        self.db.execute('INSERT INTO events(job_id,kind,generation,at) VALUES (?,?,?,?)',
                        (job, kind, generation, now))

    def enqueue(self, task: str, payload: Any, *, key: str | None = None,
                priority: int = 0, delay: float = 0, max_attempts: int = 3) -> str:
        if not isinstance(task, str) or not task:
            raise ValueError('task must be a nonempty string')
        if key is not None and (not isinstance(key, str) or not key):
            raise ValueError('key must be a nonempty string or None')
        if type(priority) is not int or type(max_attempts) is not int or max_attempts < 1:
            raise ValueError('priority must be integer; max_attempts must be positive integer')
        if not math.isfinite(delay) or delay < 0:
            raise ValueError('delay must be finite and nonnegative')
        encoded = canonical(payload)
        request = canonical([task, payload, priority, delay, max_attempts])
        with self._tx() as now:
            if key is not None:
                previous = self.db.execute('SELECT id,request FROM jobs WHERE idem_key=?', (key,)).fetchone()
                if previous is not None:
                    if previous['request'] != request:
                        raise Conflict(f'Idempotency key {key!r} has different content/options')
                    return previous['id']
            job = uuid.uuid4().hex
            self.db.execute('''INSERT INTO jobs
                (id,idem_key,request,task,payload,state,priority,available,created,max_attempts)
                VALUES (?,?,?,?,?,'ready',?,?,?,?)''',
                (job, key, request, task, encoded, priority, now + delay, now, max_attempts))
            self._event(job, 'enqueued', 0, now)
            return job

    def _recover(self, now: float) -> int:
        expired = self.db.execute("SELECT * FROM jobs WHERE state='running' AND deadline<=?", (now,)).fetchall()
        for job in expired:
            state = 'dead' if job['attempts'] >= job['max_attempts'] else 'ready'
            self.db.execute('''UPDATE jobs SET state=?,token=NULL,deadline=NULL,
                available=?,error='lease expired' WHERE id=?''', (state, now, job['id']))
            self._event(job['id'], 'expired_' + state, job['generation'], now)
        return len(expired)

    def recover(self) -> int:
        with self._tx() as now:
            return self._recover(now)

    def claim(self, *, lease: float = 30) -> Claim | None:
        positive(lease, 'lease')
        with self._tx() as now:
            self._recover(now)
            row = self.db.execute('''SELECT * FROM jobs WHERE state='ready' AND available<=?
                ORDER BY priority DESC,available,created,id LIMIT 1''', (now,)).fetchone()
            if row is None:
                return None
            token, generation = uuid.uuid4().hex, row['generation'] + 1
            self.db.execute('''UPDATE jobs SET state='running',token=?,deadline=?,
                generation=?,attempts=attempts+1 WHERE id=?''',
                (token, now + lease, generation, row['id']))
            self._event(row['id'], 'claimed', generation, now)
            return Claim(row['id'], row['task'], json.loads(row['payload']), token,
                         generation, row['attempts'] + 1, now + lease)

    def _owned(self, claim: Claim, now: float) -> sqlite3.Row:
        row = self.db.execute('''SELECT * FROM jobs WHERE id=? AND state='running'
            AND token=? AND generation=? AND deadline>?''',
            (claim.id, claim.token, claim.generation, now)).fetchone()
        if row is None:
            raise LeaseLost(f'Lease expired or superseded for {claim.id}')
        return row

    def ack(self, claim: Claim, result: Any = None) -> None:
        result_json = canonical(result)
        with self._tx() as now:
            self._owned(claim, now)
            self.db.execute("UPDATE jobs SET state='done',token=NULL,deadline=NULL,result=?,error=NULL WHERE id=?",
                            (result_json, claim.id))
            self._event(claim.id, 'done', claim.generation, now)

    def renew(self, claim: Claim, *, lease: float = 30) -> float:
        positive(lease, 'lease')
        with self._tx() as now:
            row = self._owned(claim, now)
            # Renewal cannot shorten an existing lease.
            deadline = max(row['deadline'], now + lease)
            self.db.execute('UPDATE jobs SET deadline=? WHERE id=?', (deadline, claim.id))
            self._event(claim.id, 'renewed', claim.generation, now)
            return deadline

    def fail(self, claim: Claim, error: str, *, base_delay: float = 1, cap: float = 60) -> str:
        positive(base_delay, 'base_delay')
        positive(cap, 'cap')
        if not isinstance(error, str):
            raise ValueError('error must be a string')
        with self._tx() as now:
            row = self._owned(claim, now)
            state = 'dead' if row['attempts'] >= row['max_attempts'] else 'ready'
            delay = min(cap, base_delay * (2 ** min(row['attempts'] - 1, 30)))
            self.db.execute('''UPDATE jobs SET state=?,token=NULL,deadline=NULL,error=?,
                available=? WHERE id=?''', (state, error[:4000], now + delay, claim.id))
            self._event(claim.id, 'failed_' + state, claim.generation, now)
            return state

    def get(self, job: str) -> dict[str, Any] | None:
        row = self.db.execute('SELECT * FROM jobs WHERE id=?', (job,)).fetchone()
        if row is None:
            return None
        out = dict(row)
        for name in ('payload', 'result'):
            if out[name] is not None:
                out[name] = json.loads(out[name])
        out.pop('request')
        return out

    def stats(self) -> dict[str, int]:
        counts = dict.fromkeys(('ready', 'running', 'done', 'dead'), 0)
        counts.update({r['state']: r['n'] for r in self.db.execute('SELECT state,count(*) n FROM jobs GROUP BY state')})
        return counts

    def events(self, job: str) -> list[dict[str, Any]]:
        return [dict(r) for r in self.db.execute('SELECT * FROM events WHERE job_id=? ORDER BY seq', (job,))]
