"""Fixed backlog, spawn workers, barrier start; setup/startup excluded from drain time."""

import argparse
import csv
import json
import math
import multiprocessing as mp
import os
import platform
import queue as queue_module
import random
import sqlite3
import statistics
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from durableq import Queue


def percentile(values, p):
    values = sorted(values)
    pos = (len(values) - 1) * p
    low, high = math.floor(pos), math.ceil(pos)
    return values[low] + (values[high] - values[low]) * (pos - low)


def consumer(path, sync, start, ready, output, work_ms):
    try:
        with Queue(path, synchronous=sync) as queue:
            ready.put(True)
            if not start.wait(30):
                raise TimeoutError('start barrier timed out')
            claims, acks, done = [], [], 0
            while True:
                t0 = time.perf_counter()
                claim = queue.claim(lease=60)
                t1 = time.perf_counter()
                if claim is None:
                    break
                claims.append((t1 - t0) * 1000)
                if work_ms:
                    time.sleep(work_ms / 1000)
                t2 = time.perf_counter()
                queue.ack(claim, claim.payload['index'])
                acks.append((time.perf_counter() - t2) * 1000)
                done += 1
            finished = time.perf_counter()
        output.put({'done': done, 'claims': claims, 'acks': acks, 'finished': finished})
    except BaseException as error:
        output.put({'error': repr(error)})


def trial(n, workers, sync, journal, work_ms):
    ctx = mp.get_context('spawn')
    with tempfile.TemporaryDirectory(prefix='durableq-bench-') as temp:
        path = Path(temp) / 'benchmark.db'
        Queue.initialize(path, journal=journal)
        with Queue(path, synchronous=sync) as queue:
            before = time.perf_counter()
            for index in range(n):
                queue.enqueue('bench', {'index': index}, key=f'job-{index}')
            enqueue_seconds = time.perf_counter() - before
        start, ready, output = ctx.Event(), ctx.Queue(), ctx.Queue()
        processes = [ctx.Process(target=consumer, args=(str(path), sync, start, ready, output, work_ms))
                     for _ in range(workers)]
        try:
            for process in processes:
                process.start()
            for _ in processes:
                ready.get(timeout=30)
            t0 = time.perf_counter()
            start.set()
            results = [output.get(timeout=90) for _ in processes]
            errors = [r['error'] for r in results if 'error' in r]
            if errors:
                raise RuntimeError(errors)
            duration = max(r['finished'] for r in results) - t0
            for process in processes:
                process.join(10)
                if process.exitcode != 0:
                    raise RuntimeError(f'worker exit code {process.exitcode}')
            with Queue(path) as queue:
                counts = queue.stats()
                claimed = queue.db.execute("SELECT count(*) FROM events WHERE kind='claimed'").fetchone()[0]
                integrity = queue.db.execute('PRAGMA integrity_check').fetchone()[0]
            if counts != dict(ready=0, running=0, done=n, dead=0) or claimed != n or integrity != 'ok':
                raise AssertionError(f'invariant failed: {counts}, claims={claimed}, integrity={integrity}')
            claims = [x for r in results for x in r['claims']]
            acks = [x for r in results for x in r['acks']]
            return dict(workers=workers, synchronous=sync, journal=journal, jobs=n,
                        work_ms=work_ms, elapsed_seconds=duration, jobs_per_second=n / duration,
                        enqueue_jobs_per_second=n / enqueue_seconds,
                        claim_p50_ms=percentile(claims, .5), claim_p95_ms=percentile(claims, .95),
                        claim_p99_ms=percentile(claims, .99), ack_p95_ms=percentile(acks, .95),
                        worker_counts=[r['done'] for r in results], claimed=claimed, completed=counts['done'])
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join()
            for channel in (ready, output):
                channel.close()
                channel.join_thread()


def chart(rows, path):
    # SVG is generated directly: no plotting dependency and no remote assets.
    groups = sorted({(r['synchronous'], r['workers']) for r in rows})
    summaries = [(sync, workers, statistics.mean(r['jobs_per_second'] for r in rows
                  if (r['synchronous'], r['workers']) == (sync, workers))) for sync, workers in groups]
    scale = 520 / max(v for _, _, v in summaries)
    lines = ['<svg xmlns="http://www.w3.org/2000/svg" width="820" height="370" viewBox="0 0 820 370">',
             '<rect width="820" height="370" fill="#0f172a"/>',
             '<g font-family="sans-serif" fill="#e2e8f0">',
             '<text x="24" y="34" font-size="22">DurableQ: measured backlog drain throughput</text>',
             '<text x="24" y="60" font-size="13">Mean of repeated runs; local machine, no cross-system claim</text>']
    for index, (sync, workers, value) in enumerate(summaries):
        y = 95 + index * 38
        color = '#38bdf8' if sync == 'FULL' else '#a78bfa'
        lines.extend([f'<text x="24" y="{y + 17}" font-size="14">{sync} / {workers} workers</text>',
                      f'<rect x="185" y="{y}" width="{value * scale:.1f}" height="25" rx="4" fill="{color}"/>',
                      f'<text x="{195 + value * scale:.1f}" y="{y + 17}" font-size="13">{value:.1f}/s</text>'])
    lines.extend(['<text x="24" y="350" font-size="12">FULL and NORMAL have different durability assumptions. See docs/EXPERIMENTS.md.</text>', '</g></svg>'])
    path.write_text('\n'.join(lines), encoding='utf-8')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=200)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--workers', type=int, nargs='+', default=[1, 2, 4])
    parser.add_argument('--work-ms', type=float, default=2)
    parser.add_argument('--journal', choices=['DELETE', 'WAL'], default='DELETE')
    parser.add_argument('--output', type=Path, default=Path('results'))
    args = parser.parse_args()
    if args.jobs < 1 or args.repeats < 2 or any(w < 1 for w in args.workers) or args.work_ms < 0:
        parser.error('positive jobs/workers, at least two repeats, nonnegative work-ms required')
    rng = random.Random(20261004)
    configs = [(sync, workers, repeat) for sync in ['FULL', 'NORMAL']
               for workers in args.workers for repeat in range(args.repeats)]
    rng.shuffle(configs)
    rows = []
    for sync, workers, repeat in configs:
        result = trial(args.jobs, workers, sync, args.journal, args.work_ms)
        result['repeat'] = repeat
        rows.append(result)
        print(f'{sync} workers={workers} repeat={repeat}: {result["jobs_per_second"]:.1f} jobs/s', flush=True)
    args.output.mkdir(parents=True, exist_ok=True)
    metadata = dict(created_utc=datetime.now(timezone.utc).isoformat(), python=platform.python_version(),
                    sqlite=sqlite3.sqlite_version, os=platform.platform(), logical_cpus=os.cpu_count(),
                    shuffle_seed=20261004, clock='perf_counter', start_method='spawn',
                    repeats=args.repeats, jobs_per_trial=args.jobs, work_ms=args.work_ms,
                    journal=args.journal, raw_trials=rows)
    (args.output / 'benchmark.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    with (args.output / 'benchmark.csv').open('w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    report = ['# 实测性能结果', '', f'环境：Python {metadata["python"]} / SQLite {metadata["sqlite"]} / {metadata["os"]}', '',
              f'每次 {args.jobs} 个任务，模拟处理耗时 {args.work_ms} ms，每组 {args.repeats} 次；顺序随机化。', '',
              '| 同步策略 | Worker | 吞吐均值 ± 样本标准差 (jobs/s) | claim P95 均值 (ms) |',
              '|---|---:|---:|---:|']
    for sync in ['FULL', 'NORMAL']:
        for workers in sorted(set(args.workers)):
            group = [r for r in rows if (r['synchronous'], r['workers']) == (sync, workers)]
            values = [r['jobs_per_second'] for r in group]
            report.append(f'| {sync} | {workers} | {statistics.mean(values):.2f} ± {statistics.stdev(values):.2f} | '
                          f'{statistics.mean(r["claim_p95_ms"] for r in group):.3f} |')
    report += ['', '![Measured throughput](throughput.svg)', '',
               '这些是单机小样本观测，不代表生产容量。吞吐计时排除初始化、入队和进程启动；',
               '包括唤醒、任务领取、模拟处理、确认、最终空队列轮询，不包括统计校验。',
               'NORMAL 仅作性能消融，不能据此宣称具有与 FULL 相同的掉电耐久性。',
               '所有 trial 均校验完成数量、claim 数量和 SQLite integrity_check。',
               '磁盘型号、实际 fsync 行为、后台负载和缓存状态未受控，跨机器比较需重新测量。']
    (args.output / 'REPORT.md').write_text('\n'.join(report) + '\n', encoding='utf-8')
    chart(rows, args.output / 'throughput.svg')


if __name__ == '__main__':
    main()
