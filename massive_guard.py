"""FAISAL_MASSIVE_SHARED_GATE_V1.

One best-effort cross-worker rate budget shared by live /watch and the silent
source diagnostic. Render Linux/one-instance local filesystem only. If shared
state cannot be read/written, fail closed (skip Massive; keep Yahoo working).
No tokens, URLs or credentials are written to disk or logs.
"""
import json
import os
import time
from pathlib import Path

import requests


def _state_file():
    base = os.environ.get('WATCH_STATE_FILE') or '/tmp/faisal_watch_state.json'
    return Path(base + '.massive_gate.json')


def _int_env(name, default, lo, hi):
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return min(hi, max(lo, value))


def _edit(mutator, *, lock_nonblocking=False):
    """Atomic read/mutate/write under process-wide Linux flock."""
    import fcntl
    path = _state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = str(path) + '.lock'
    with open(lock_path, 'a+', encoding='utf-8') as lock:
        how = fcntl.LOCK_EX | (fcntl.LOCK_NB if lock_nonblocking else 0)
        fcntl.flock(lock, how)
        try:
            with path.open(encoding='utf-8') as handle:
                state = json.load(handle)
                if not isinstance(state, dict):
                    raise ValueError('invalid Massive state')
        except FileNotFoundError:
            state = {}
        result = mutator(state)
        tmp = path.with_name(f'{path.name}.{os.getpid()}.{__import__("threading").get_ident()}.tmp')
        try:
            with tmp.open('w', encoding='utf-8') as handle:
                json.dump(state, handle)
            os.replace(tmp, path)
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
        return result


def _reserve():
    now = time.time()
    limit = _int_env('MASSIVE_MAX_REQUESTS_PER_MIN', 4, 1, 60)

    def update(state):
        if now < float(state.get('cooldown_until') or 0):
            return '429_cooldown'
        events = state.get('requests', [])
        if not isinstance(events, list):
            events = []
        events = [t for t in events if isinstance(t, (float, int)) and 0 <= now - t < 60]
        state['requests'] = events
        if len(events) >= limit:
            return 'minute_budget'
        events.append(now)
        state['requests'] = events
        return 'allowed'

    try:
        return _edit(update, lock_nonblocking=True)
    except Exception as exc:
        # If the gate cannot protect the budget, do not bypass it.
        print(f'[massive-gate] cannot reserve ({type(exc).__name__}); skipped')
        return 'gate_unavailable'


def _handle_429(response):
    pause = _int_env('MASSIVE_429_COOLDOWN_SEC', 180, 120, 900)
    try:
        retry = float(response.headers.get('Retry-After', '0'))
        if retry > pause:
            pause = min(900, int(retry))
    except (AttributeError, ValueError, TypeError):
        pass
    deadline = time.time() + pause

    def update(state):
        state['cooldown_until'] = max(deadline, float(state.get('cooldown_until') or 0))
    try:
        _edit(update)
    except Exception as exc:
        print(f'[massive-gate] cannot save cooldown ({type(exc).__name__})')
    print(f'[massive-gate] HTTP 429; shared cooldown {pause}s')


def guarded_get(url, *, params=None, timeout=12):
    """Return (HTTP response or None, reason). Uses guarded shared budget.

    A skipped request never means the symbol had no trades. The caller must
    never treat it as evidence of price freshness or execute trade decisions.
    """
    outcome = _reserve()
    if outcome != 'allowed':
        return None, f'skipped_{outcome}'
    response = requests.get(url, params=params, timeout=timeout)
    if response.status_code == 429:
        _handle_429(response)
    return response, f'http_{response.status_code}'
