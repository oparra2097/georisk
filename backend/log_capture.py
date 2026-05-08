"""
In-memory ring buffer for recent log records.

Attaches to the root logger so anything `logger.info(...)` from any module
gets stored in a bounded deque. Lets the /debug endpoints return the last
~200 log lines without requiring SSH into Render.
"""

import logging
import time
from collections import deque
from threading import RLock

_BUFFER_SIZE = 300
_buffer: deque[dict] = deque(maxlen=_BUFFER_SIZE)
_lock = RLock()
_attached = False


class _MemoryHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        with _lock:
            _buffer.append({
                't': time.time(),
                'level': record.levelname,
                'logger': record.name,
                'msg': msg[:1500],
            })


def install():
    """Idempotent: attach the memory handler to the root logger once."""
    global _attached
    if _attached:
        return
    handler = _MemoryHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logging.getLogger().addHandler(handler)
    # Make sure root level lets INFO through (Flask sometimes leaves it at WARNING)
    logging.getLogger().setLevel(logging.INFO)
    _attached = True


def snapshot(filter_substr: str = '') -> list[dict]:
    """Return a copy of the buffer; filter to records whose message contains substr."""
    with _lock:
        items = list(_buffer)
    if filter_substr:
        items = [r for r in items if filter_substr in r['msg'] or filter_substr in r['logger']]
    return items
