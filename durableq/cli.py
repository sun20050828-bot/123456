"""Small operational CLI with explicit configuration and machine-readable output."""

import argparse
import json
import sqlite3
import sys

from .store import Conflict, Queue
from .worker import run_once


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='durableq')
    parser.add_argument('--db', default='queue.db')
    sub = parser.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init')
    init.add_argument('--journal', choices=['DELETE', 'WAL'], default='DELETE')
    enqueue = sub.add_parser('enqueue')
    enqueue.add_argument('task', choices=['echo', 'sum'])
    enqueue.add_argument('payload', help='JSON payload')
    enqueue.add_argument('--key')
    enqueue.add_argument('--delay', type=float, default=0)
    enqueue.add_argument('--priority', type=int, default=0)
    enqueue.add_argument('--max-attempts', type=int, default=3)
    work = sub.add_parser('work', help='Process up to N currently eligible jobs; exits on idle')
    work.add_argument('--limit', type=int, default=100)
    work.add_argument('--lease', type=float, default=30)
    sub.add_parser('stats')
    sub.add_parser('recover')
    show = sub.add_parser('show')
    show.add_argument('id')
    args = parser.parse_args(argv)
    try:
        if args.command == 'init':
            Queue.initialize(args.db, journal=args.journal)
            output = {'database': args.db, 'journal': args.journal}
        else:
            with Queue(args.db) as queue:
                if args.command == 'enqueue':
                    output = {'id': queue.enqueue(args.task, json.loads(args.payload), key=args.key,
                              delay=args.delay, priority=args.priority, max_attempts=args.max_attempts)}
                elif args.command == 'work':
                    if args.limit < 1:
                        raise ValueError('limit must be positive')
                    output = {'done': 0, 'failed': 0, 'lease_lost': 0}
                    for _ in range(args.limit):
                        status = run_once(queue, {'echo': lambda c: c.payload,
                                                 'sum': lambda c: sum(c.payload)}, lease=args.lease)
                        if status == 'idle':
                            break
                        output[status] += 1
                elif args.command == 'stats':
                    output = queue.stats()
                elif args.command == 'recover':
                    output = {'recovered': queue.recover()}
                else:
                    output = queue.get(args.id)
                    if output is None:
                        raise ValueError('job not found')
        print(json.dumps(output, ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, sqlite3.Error) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
