"""
Shared cache helper for the data-source layer.

Every fetcher under ``backend/data_sources/`` previously reinvented the
same TTL / in-memory dict / disk-backed JSON pattern:

    _cache = {}
    _cache_lock = threading.Lock()
    _CACHE_TTL = 86400
    _DISK_CACHE_DIR = os.path.join(Config.DATA_DIR, 'wb_cache')

    def get_thing(key):
        with _cache_lock:
            entry = _cache.get(key)
            if entry and time.time() - entry['ts'] < _CACHE_TTL:
                return entry['data']
        # ... disk read, then fetch, then disk write ...

This module centralises that so:

- One place to fix a cache bug.
- One place to add optional Redis / on-disk-LRU / etc.
- Consistent atomic disk writes (`.tmp` + `os.replace`).
- Consistent stale-serve fallback (return old data when the fetch fails
  and we have a prior success on hand — matches the pattern in
  ``world_bank.py`` which most callers should also be doing).

Public API is minimal:

    from backend.data_sources._cache import cached

    @cached(namespace='wb', ttl=86400, disk=True)
    def get_wb_data(indicator, source=None):
        ...
        return {'countries': ..., 'meta': ...}

The decorator handles cache hit/miss, atomic disk persistence, per-key
locking (so concurrent gunicorn workers don't dogpile the upstream API),
and stale-serve on failure. See ``@cached`` docstring for options.

For raw access without the decorator:

    from backend.data_sources._cache import cache_get, cache_put

    hit, value = cache_get('wb', 'NY.GDP.MKTP.CD', ttl=86400)
    if hit:
        return value
    fresh = fetch_from_wb(...)
    cache_put('wb', 'NY.GDP.MKTP.CD', fresh, disk=True)
    return fresh
"""

from __future__ import annotations

import functools
import json
import os
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

try:
    from config import Config
    _DATA_DIR = getattr(Config, 'DATA_DIR', None) or os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data',
    )
except Exception:  # noqa: BLE001
    _DATA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data',
    )

# Repo cache dir — committed by the GH Actions refresh workflow. Serves
# as a warm-seed source on cold boot; every namespace read checks BOTH
# this location and Config.DATA_DIR and prefers the newer entry.
_REPO_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'cache',
)


def _write_root() -> str:
    """Directory where the ``@cached`` decorator's disk writes land.

    Priority: ``CACHE_DIR_OVERRIDE`` env var (used by
    ``scripts/refresh_cache.py`` to write directly into
    ``data/cache/`` under the repo) → live ``_DATA_DIR``. Resolved
    per-call so the env can be flipped in tests without a reimport.
    """
    return os.environ.get('CACHE_DIR_OVERRIDE') or _DATA_DIR


# ── Per-namespace in-memory store ───────────────────────────────────────
# _STORE[namespace] = {key: {'data': ..., 'ts': float}}. Each namespace
# has its own lock so a slow read on 'wb' doesn't block writes on 'imf'.

_STORE: Dict[str, Dict[str, Dict[str, Any]]] = {}
_LOCKS: Dict[str, threading.Lock] = {}
_LOCKS_LOCK = threading.Lock()


def _lock_for(namespace: str) -> threading.Lock:
    with _LOCKS_LOCK:
        lock = _LOCKS.get(namespace)
        if lock is None:
            lock = threading.Lock()
            _LOCKS[namespace] = lock
            _STORE.setdefault(namespace, {})
        return lock


def _disk_path(namespace: str, key: str, root: Optional[str] = None) -> str:
    safe_key = key.replace('/', '_').replace('\\', '_')
    return os.path.join(root or _write_root(), f'{namespace}_cache', f'{safe_key}.json')


def _load_from_disk(namespace: str, key: str) -> Tuple[Optional[Any], float]:
    """Return ``(data, mtime)`` for the newest available disk entry.

    Checks both ``_write_root()/<namespace>_cache/<key>.json`` (live
    Flask writes / persistent disk) and ``_REPO_CACHE_DIR/<namespace>_cache/
    <key>.json`` (GH Actions committed snapshots) and returns whichever
    has the newer ``ts`` payload. This lets the GH Actions refresh
    warm-seed every namespace at deploy time without displacing
    fresher live writes.
    """
    candidates: list[Tuple[Optional[Any], float]] = []
    paths = {_disk_path(namespace, key), _disk_path(namespace, key, root=_REPO_CACHE_DIR)}
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            with open(path, 'r') as f:
                payload = json.load(f)
            candidates.append((payload.get('data'), float(payload.get('ts') or 0.0)))
        except (OSError, ValueError, json.JSONDecodeError) as e:
            print(f'[_cache] disk read failed for {namespace}/{key} at {path}: {e}')
    if not candidates:
        return None, 0.0
    # Return the newest.
    return max(candidates, key=lambda pair: pair[1])


def _save_to_disk(namespace: str, key: str, data: Any, ts: float) -> None:
    """Atomically write ``data`` to the namespace disk cache. Failures
    are logged, never raised — the in-memory cache still holds the entry
    so this only affects survival across restarts."""
    path = _disk_path(namespace, key)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'ts': ts, 'data': data}, f)
        os.replace(tmp, path)
    except OSError as e:
        print(f'[_cache] disk write failed for {namespace}/{key}: {e}')


# ── Public raw API ──────────────────────────────────────────────────────


def cache_get(namespace: str, key: str, ttl: float,
              disk: bool = True) -> Tuple[bool, Any]:
    """Return ``(hit, value)``. ``hit=True`` iff we have a fresh entry
    within ``ttl`` seconds. When ``disk=True`` and the in-memory entry is
    missing/stale, we try the disk cache next before returning a miss."""
    lock = _lock_for(namespace)
    with lock:
        entry = _STORE.get(namespace, {}).get(key)
        if entry and (time.time() - entry['ts']) < ttl:
            return True, entry['data']

    if disk:
        data, ts = _load_from_disk(namespace, key)
        if data is not None and (time.time() - ts) < ttl:
            # Promote to in-memory so future calls skip the disk read.
            with lock:
                _STORE.setdefault(namespace, {})[key] = {'data': data, 'ts': ts}
            return True, data

    return False, None


def cache_put(namespace: str, key: str, data: Any,
              disk: bool = True) -> None:
    """Store ``data`` in the in-memory cache and (optionally) on disk."""
    ts = time.time()
    lock = _lock_for(namespace)
    with lock:
        _STORE.setdefault(namespace, {})[key] = {'data': data, 'ts': ts}
    if disk:
        _save_to_disk(namespace, key, data, ts)


def stale_read(namespace: str, key: str, disk: bool = True) -> Optional[Any]:
    """Read whatever data is on hand, ignoring TTL. Used to serve stale
    content when a fresh fetch fails."""
    lock = _lock_for(namespace)
    with lock:
        entry = _STORE.get(namespace, {}).get(key)
        if entry:
            return entry['data']
    if disk:
        data, _ts = _load_from_disk(namespace, key)
        return data
    return None


def invalidate(namespace: str, key: Optional[str] = None) -> None:
    """Drop an entry (or an entire namespace) from the in-memory cache.
    Disk copies survive so a redeploy can still warm-cache from them."""
    lock = _lock_for(namespace)
    with lock:
        if key is None:
            _STORE[namespace] = {}
        else:
            _STORE.get(namespace, {}).pop(key, None)


# ── Decorator ───────────────────────────────────────────────────────────


def cached(namespace: str, ttl: float, disk: bool = True,
           serve_stale_on_error: bool = True) -> Callable:
    """Decorator that caches the return value of a function.

    ``namespace`` — logical bucket (usually the fetcher name, e.g. 'wb',
                     'imf_weo', 'em_reserves').
    ``ttl`` — seconds; entries older than this are refetched.
    ``disk`` — if True, also read/write JSON on disk (survives restarts,
               shared across gunicorn workers via `Config.DATA_DIR`).
    ``serve_stale_on_error`` — if the wrapped function raises, we fall
               back to whatever's cached (even if stale) rather than
               propagating the exception. Set False for functions where
               a stale response is worse than an error.

    Cache key is derived from the positional and keyword args via `repr()`.
    Keep args JSON-serialisable if you want disk survival.
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            key = _cache_key(fn.__name__, args, kwargs)
            hit, value = cache_get(namespace, key, ttl, disk=disk)
            if hit:
                return value
            try:
                fresh = fn(*args, **kwargs)
            except Exception as e:  # noqa: BLE001
                if serve_stale_on_error:
                    stale = stale_read(namespace, key, disk=disk)
                    if stale is not None:
                        print(f'[_cache] {namespace}/{key} serving stale after error: {e}')
                        return stale
                raise
            cache_put(namespace, key, fresh, disk=disk)
            return fresh
        return wrapper
    return decorator


def _cache_key(fn_name: str, args: tuple, kwargs: dict) -> str:
    """Deterministic key that fits a filesystem — no slashes, no colons.
    Uses `repr()` on args + sorted kwargs; primitives serialise cleanly.
    """
    parts = [fn_name]
    for a in args:
        parts.append(repr(a))
    for k in sorted(kwargs.keys()):
        parts.append(f'{k}={kwargs[k]!r}')
    joined = '__'.join(parts)
    # Strip path-hostile characters.
    safe = ''.join(c if c.isalnum() or c in '._-' else '_' for c in joined)
    # Filesystem name-length limits kick in around 255; hash the excess.
    if len(safe) > 200:
        import hashlib
        h = hashlib.sha1(safe.encode('utf-8')).hexdigest()[:12]
        safe = safe[:180] + '__' + h
    return safe
