"""FAISAL_ALPACA_SOURCE_DIAG_V1: read-only, silent Yahoo/Massive/Alpaca IEX diagnostic.

No trades, no Telegram, no changes to primary quote source or /watch signals.
Runs at most once per 30-minute slot across Render workers, using an advisory file lock.
"""
import os
import time
import threading
import json
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import massive_guard as _massive_guard  # FAISAL_MASSIVE_SHARED_GATE_V1
from collections import Counter

import requests


def _latest_age(ts, now=None):
    """Age in minutes or None; supports Unix timestamps and Alpaca RFC3339 strings."""
    now = time.time() if now is None else now
    try:
        if isinstance(ts, str):
            s = ts.strip().replace('Z', '+00:00')
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                return None
            ts = dt.astimezone(timezone.utc).timestamp()
        return (float(now) - float(ts)) / 60.0
    except (TypeError, ValueError, OverflowError):
        return None


def _label_age(ts, now=None):
    age = _latest_age(ts, now)
    if age is None:
        return 'missing'
    if age < -1:
        return 'future'
    if age <= 20:
        return 'fresh'
    return 'stale'


def _status_session(result, err, sessions, fresh):
    if err:
        return '429' if '429' in str(err) else 'error'
    if not result:
        return 'no_bars'
    try:
        s = sessions(result)
    except Exception:
        return 'invalid_sessions'
    if not s:
        # Bars can exist yet be unusable for the monitor's prior-close/reference logic.
        return 'invalid_sessions'
    try:
        return 'fresh' if fresh(s) else ('stale' if s.get('last_ts') else 'invalid_sessions')
    except Exception:
        return 'invalid_sessions'


def _select_sample(symbols, missing, slot, miss_cap=5, ok_cap=2):
    all_syms = sorted(set(symbols))
    miss = sorted(set(missing) & set(all_syms))
    ok = sorted(set(all_syms) - set(miss))

    def rotate(items, cap, seed):
        if not items:
            return []
        count = min(len(items), cap)
        start = (int(seed) * count) % len(items)
        return [items[(start + i) % len(items)] for i in range(count)]

    return rotate(miss, miss_cap, slot) + rotate(ok, ok_cap, slot), set(miss)


def _claim_slot(state_path, slot):
    """Prevents repeated probes across different uvicorn workers in the same slot."""
    try:
        import fcntl
        path = state_path + '.source_diag_slot'
        with open(path, 'a+', encoding='ascii') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.seek(0)
            if f.read().strip() == str(slot):
                return False
            f.seek(0)
            f.truncate()
            f.write(str(slot))
            f.flush()
            return True
    except Exception as e:
        # The monitoring path remains fully operational if diagnostics cannot lock.
        print(f'[source-diag] slot lock unavailable ({type(e).__name__}); skipping')
        return False


def _alpaca_latest(symbols, now=None):
    """One batched IEX request; never includes API credentials in logs."""
    key = os.environ.get('APCA_API_KEY_ID') or os.environ.get('ALPACA_API_KEY_ID')
    secret = os.environ.get('APCA_API_SECRET_KEY') or os.environ.get('ALPACA_API_SECRET_KEY')
    if not key or not secret:
        return 'missing_credentials', {}, {}
    try:
        resp = requests.get(
            'https://data.alpaca.markets/v2/stocks/bars/latest',
            params={'symbols': ','.join(symbols), 'feed': 'iex'},
            headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret},
            timeout=12,
        )
        if resp.status_code != 200:
            return f'http_{resp.status_code}', {}, {}
        bars = (resp.json() or {}).get('bars') or {}
        if not isinstance(bars, dict):
            return 'bad_response', {}, {}
        return ('ok',
                {s: _label_age((bars.get(s) or {}).get('t'), now) for s in symbols},
                {s: _latest_age((bars.get(s) or {}).get('t'), now) for s in symbols})
    except Exception as exc:
        return f'network_{type(exc).__name__}', {}, {}


# FAISAL_MASSIVE_HTTP_AGE_DIAG_V2: diagnostic-only Massive request.
# No changes to _massive_result used by live /watch, no Telegram, no key logging.
def _age_text(age):
    return 'na' if age is None else f'{age:.1f}'


def _massive_probe(sym, now):
    key = os.environ.get('MASSIVE_API_KEY')
    if not key:
        return 'not_configured', None
    try:
        day = datetime.fromtimestamp(now, ZoneInfo('America/New_York')).date()
        start_day = str(day - timedelta(days=5))
        # Mirror app.py original parameters; only the diagnostic result is logged.
        resp, gate = _massive_guard.guarded_get(
            f'https://api.massive.com/v2/aggs/ticker/{sym.replace(".", "-")}/range/5/minute/{start_day}/{day}',
            params={'adjusted': 'true', 'sort': 'asc', 'limit': 5000, 'apiKey': key},
            timeout=12,
        )
        if resp is None:
            return gate, None  # Rate-limited skip, not evidence of no trades
        code = int(resp.status_code)
        if code != 200:
            return f'http_{code}', None
        data = resp.json()
        if not isinstance(data, dict):
            return 'http_200_invalid_json', None
        rows = data.get('results') or []
        if not isinstance(rows, list):
            return 'http_200_invalid_rows', None
        if not rows:
            return 'http_200_no_bars', None
        ts_ms = rows[-1].get('t') if isinstance(rows[-1], dict) else None
        age = _latest_age(float(ts_ms) / 1000 if ts_ms is not None else None, now)
        if age is None:
            return 'http_200_invalid_timestamp', None
        return f'http_200_{_label_age(float(ts_ms) / 1000, now)}', age
    except requests.RequestException as exc:
        return f'network_{type(exc).__name__}', None
    except Exception as exc:
        return f'parse_{type(exc).__name__}', None


def _run_sample(symbols, originally_missing, chart, massive, sessions, fresh):
    start = time.time()
    now = time.time()
    iex_request, iex_results, iex_ages = _alpaca_latest(symbols, now)
    counts = {'yahoo': Counter(), 'massive': Counter(), 'iex': Counter()}
    rescued = []
    for sym in symbols:
        try:
            y, err = chart(sym, '2d', '5m', True)
            ystate = _status_session(y, err, sessions, fresh)
            yage = _latest_age((y.get('timestamp') or [None])[-1], now) if y else None
        except Exception:
            ystate, yage = 'exception', None
        # Fresh Yahoo: do not spend a Massive API call on a redundant comparison.
        if ystate == 'fresh':
            mstate, mage = 'skipped_yahoo_fresh', None
        else:
            mstate, mage = _massive_probe(sym, now)
        istate = iex_results.get(sym, 'missing') if iex_request == 'ok' else iex_request
        for src, status in [('yahoo', ystate), ('massive', mstate), ('iex', istate)]:
            counts[src][status] += 1
        if sym in originally_missing and istate == 'fresh':
            rescued.append(sym)
        print(f'[source-diag] {sym} watch_missing={int(sym in originally_missing)} '
              f'yahoo={ystate} yahoo_age_min={_age_text(yage)} '
              f'massive={mstate} massive_age_min={_age_text(mage)} '
              f'iex={istate} iex_age_min={_age_text(iex_ages.get(sym))}')
    def fmt(c):
        return ','.join(f'{k}:{c[k]}' for k in sorted(c)) or 'none'
    print(f'[source-diag] summary n={len(symbols)} originally_missing={len(originally_missing & set(symbols))} '
          f'yahoo[{fmt(counts["yahoo"])}] massive[{fmt(counts["massive"])}] '
          f'iex[{fmt(counts["iex"])}] iex_fresh_on_missing={len(rescued)} '
          f'iex_status={iex_request} elapsed_s={time.time()-start:.1f}')


def schedule_source_diagnostic(symbols, missing, chart, massive, sessions, fresh, state_path):
    """Non-blocking, no changes to /watch outcomes. Only sample 7 symbols / 30 min."""
    if str(os.environ.get('FAISAL_SOURCE_DIAG', '1')).lower() in ('0', 'false', 'off'):
        return False
    slot = int(time.time() // 1800)
    if not _claim_slot(state_path, slot):
        return False
    chosen, missing_set = _select_sample(symbols, missing, slot)
    if not chosen:
        print('[source-diag] empty watchlist sample')
        return False
    if not ((os.environ.get('APCA_API_KEY_ID') or os.environ.get('ALPACA_API_KEY_ID')) and
            (os.environ.get('APCA_API_SECRET_KEY') or os.environ.get('ALPACA_API_SECRET_KEY'))):
        print('[source-diag] disabled: add APCA_API_KEY_ID and APCA_API_SECRET_KEY in Render Environment')
        return False
    worker = threading.Thread(
        target=_run_sample,
        args=(chosen, missing_set, chart, massive, sessions, fresh),
        daemon=True,
        name='faisal-source-diag',
    )
    worker.start()
    return True
