# Contributing

Run `python -m unittest discover -v` from the repository root before proposing a change.
Changes to queue semantics should include a reproducible failure history and update docs/DESIGN.md.
Keep handlers outside storage transactions. Do not silently replay handler work after storage errors.

Performance claims require raw trials, environment metadata and matching durability assumptions.
Use fresh output folders for new experiments: `python -m experiments.benchmark --output results/new-run`.
Generated databases and machine secrets must not be committed.

No external runtime dependencies are required. Python 3.11+ is supported.
WAL tests run only when the Python SQLite library includes the official WAL-reset fix.
