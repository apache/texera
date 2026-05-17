"""In-memory store for upload_id → dataset context (analyze + instantiate)."""

from __future__ import annotations

import threading
import uuid
from typing import Any

_lock = threading.Lock()
_store: dict[str, dict[str, Any]] = {}


def create_upload_id() -> str:
    return str(uuid.uuid4())


def put(upload_id: str, record: dict[str, Any]) -> None:
    with _lock:
        _store[upload_id] = record


def get(upload_id: str) -> dict[str, Any] | None:
    with _lock:
        return _store.get(upload_id)


def merge(upload_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
    """Merge ``patch`` into cached record; return updated dict or None if missing."""
    with _lock:
        rec = _store.get(upload_id)
        if rec is None:
            return None
        rec.update(patch)
        return rec
