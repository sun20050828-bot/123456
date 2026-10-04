"""Handlers run outside database transactions; use job.id for effect deduplication."""

from collections.abc import Callable, Mapping
from typing import Any

from .store import Claim, LeaseLost, Queue


def run_once(queue: Queue, handlers: Mapping[str, Callable[[Claim], Any]], *, lease: float = 30) -> str:
    claim = queue.claim(lease=lease)
    if claim is None:
        return 'idle'
    try:
        handler = handlers[claim.task]
        result = handler(claim)
    except Exception as error:
        try:
            queue.fail(claim, f'{type(error).__name__}: {error}')
            return 'failed'
        except LeaseLost:
            return 'lease_lost'
    # Storage errors during ack must propagate, not be reported as handler failures.
    try:
        queue.ack(claim, result)
        return 'done'
    except LeaseLost:
        return 'lease_lost'
