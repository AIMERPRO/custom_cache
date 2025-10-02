from __future__ import annotations

import asyncio
import threading
from contextlib import contextmanager
from typing import Dict, Optional, Iterable, Any

from .custom_cache import CustomCache, default_backend


class Singleflight:
    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: Dict[str, threading.Lock] = {}

    def get_lock(self, key: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    @contextmanager
    def for_key(self, key: str):
        lock = self.get_lock(key)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def reset(self) -> None:
        with self._guard:
            self._locks.clear()


class AsyncSingleflight:
    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks_by_loop: Dict[int, Dict[str, asyncio.Lock]] = {}

    def _per_loop(self) -> Dict[str, asyncio.Lock]:
        loop = asyncio.get_running_loop()
        lid = id(loop)
        with self._guard:
            d = self._locks_by_loop.get(lid)
            if d is None:
                d = {}
                self._locks_by_loop[lid] = d
            return d

    def get_lock(self, key: str) -> asyncio.Lock:
        d = self._per_loop()
        lock = d.get(key)
        if lock is None:
            lock = asyncio.Lock()
            d[key] = lock
        return lock

    async def for_key(self, key: str):
        lock = self.get_lock(key)
        return _AsyncLockCtx(lock)

    def reset(self) -> None:
        with self._guard:
            self._locks_by_loop.clear()


class _AsyncLockCtx:
    def __init__(self, lock: asyncio.Lock) -> None:
        self._lock = lock

    async def __aenter__(self):
        await self._lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()


def resolve_tags(tags, args, kwargs) -> Optional[Any]:
    if tags is None:
        return None
    if callable(tags):
        try:
            return tags(*args, **kwargs)
        except TypeError:
            return tags(*args, **kwargs)
    return tags


def invalidate(*, tags: Iterable[str], backend: Optional[CustomCache] = None) -> None:
    store = backend or default_backend
    store.invalidate_tags(tags)
