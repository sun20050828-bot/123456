"""Exhaustive finite abstraction of token fencing; NOT a proof of the SQL implementation."""

import json
from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path


@dataclass(frozen=True)
class State:
    status: str = 'ready'
    generation: int = 0
    expired: bool = False
    held: tuple[int, ...] = ()


def explore(fenced=True, bound=2, check_expiry=False):
    initial = State()
    todo = deque([(initial, [])])
    visited = {initial}
    transitions = 0
    while todo:
        state, trace = todo.popleft()
        successors = []
        if state.status == 'ready' and state.generation < bound:
            generation = state.generation + 1
            successors.append((f'claim token-{generation}', replace(state, status='running',
                               generation=generation, expired=False, held=state.held + (generation,))))
        if state.status == 'running':
            if not state.expired:
                successors.append(('deadline expires', replace(state, expired=True)))
                successors.append(('handler fails', replace(state, status='dead' if state.generation == bound else 'ready')))
            else:
                successors.append(('recover', replace(state, status='dead' if state.generation == bound else 'ready', expired=False)))
            for token in state.held:
                valid = token == state.generation and not state.expired
                # Broken variant checks only status. Trace must expose invalid acknowledgement.
                if valid or (not fenced and (not check_expiry or not state.expired)):
                    action = f'ack token-{token}'
                    if not valid:
                        return {'safe': False, 'visited_states': len(visited), 'transitions': transitions,
                                'counterexample': trace + [action], 'attempt_bound': bound}
                    successors.append((action, replace(state, status='done')))
        for action, successor in successors:
            transitions += 1
            if successor not in visited:
                visited.add(successor)
                todo.append((successor, trace + [action]))
    return {'safe': True, 'visited_states': len(visited), 'transitions': transitions,
            'counterexample': None, 'attempt_bound': bound}


def main():
    checked, broken, unfenced = explore(True), explore(False), explore(False, check_expiry=True)
    if not checked['safe'] or broken['safe'] or unfenced['safe']:
        raise AssertionError('model checker did not distinguish fencing')
    out = {'fenced': checked, 'status_only_ack': broken, 'deadline_only_ack': unfenced,
           'scope': 'one job, two claim generations, abstract deadline; safety only; not SQL verification'}
    Path('results').mkdir(exist_ok=True)
    Path('results/model_check.json').write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(json.dumps(out, indent=2))


if __name__ == '__main__':
    main()
