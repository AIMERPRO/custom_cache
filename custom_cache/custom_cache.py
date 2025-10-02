from __future__ import annotations

import hashlib
import pickle
import threading
import time
from abc import abstractmethod, ABC
from contextlib import contextmanager
from typing import Any, Optional, Dict, Tuple, Iterable, Set

from redis import Redis
from redis.exceptions import RedisError


class CustomCache(ABC):
    """
    Базовый интерфейс для кэш-бэкендов.

    Контракт:
      - get(key) -> (hit: bool, value: Any)
      - set(key, value, ttl=None, *, tags=None) -> None
      - delete(key) -> None
      - clear() -> None
      - invalidate_tags(tags) -> None
    """

    @abstractmethod
    def get(self, key: str) -> Tuple[bool, Any]:
        raise NotImplementedError

    @abstractmethod
    def set(
            self,
            key: str,
            value: Any,
            ttl: float,
            *,
            tags: Optional[Iterable[str]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def invalidate_tags(self, tags: Iterable[str]) -> None:
        raise NotImplementedError


class InMemoryCache(CustomCache):
    def __init__(self) -> None:
        self._data: Dict[str, Tuple[Optional[float], bytes]] = {}
        self._lock = threading.Lock()
        self._tag_index: Dict[str, Set[str]] = {}
        self._key_tags: Dict[str, Set[str]] = {}

    @contextmanager
    def _locked(self):
        with self._lock:
            yield

    @staticmethod
    def _now() -> float:
        return time.monotonic()

    def _unlink_key_unlocked(self, key: str) -> None:
        tags = self._key_tags.pop(key, None)
        if not tags:
            return
        for t in tags:
            ks = self._tag_index.get(t)
            if ks is None:
                continue
            ks.discard(key)
            if not ks:
                self._tag_index.pop(t, None)

    def get(self, key: str) -> tuple[bool, Any]:
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return False, None
            expires_at, blob = item
            if expires_at is not None and expires_at <= self._now():
                self._data.pop(key, None)
                self._unlink_key_unlocked(key)
                return False, None
        return True, pickle.loads(blob)

    def set(self, key: str, value: Any, ttl: float, *, tags: Optional[Iterable[str]] = None) -> None:
        if ttl is None or ttl <= 0:
            raise ValueError("ttl must be a positive number (seconds)")

        expires_at = None if ttl is None else self._now() + float(ttl)
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        with self._lock:
            if key in self._data:
                self._unlink_key_unlocked(key)
            self._data[key] = (expires_at, blob)
            if tags:
                tset = set(tags)
                self._key_tags[key] = tset
                for t in tset:
                    s = self._tag_index.get(t)
                    if s is None:
                        s = set()
                        self._tag_index[t] = s
                    s.add(key)

    def delete(self, key: str) -> None:
        with self._locked():
            self._data.pop(key, None)
            self._unlink_key_unlocked(key)

    def clear(self) -> None:
        with self._locked():
            self._data.clear()
            self._tag_index.clear()
            self._key_tags.clear()

    def invalidate_tags(self, tags: Iterable[str]) -> None:
        with self._lock:
            keys_to_drop: Set[str] = set()
            for t in tags:
                ks = self._tag_index.get(t)
                if ks:
                    keys_to_drop.update(ks)

            for k in keys_to_drop:
                self._data.pop(k, None)
                self._unlink_key_unlocked(k)


class RedisCache(CustomCache):
    def __init__(
            self,
            client: Redis,
            *,
            value_prefix: str = "rc:",
            meta_prefix: str = "rcmeta",
            pickle_protocol: int = pickle.HIGHEST_PROTOCOL,
    ) -> None:
        self.r = client
        self.value_prefix = value_prefix.rstrip(":") + ":"
        self.meta_prefix = meta_prefix.rstrip(":")
        self.pickle_protocol = pickle_protocol

    def _ktags_key(self, value_key: str) -> str:
        h = hashlib.sha1(value_key.encode("utf-8")).hexdigest()
        return f"{self.meta_prefix}:kt:{h}"

    def _tagset_key(self, tag: str) -> str:
        return f"{self.meta_prefix}:tag:{tag}"

    def get(self, key: str) -> tuple[bool, Any]:
        try:
            blob = self.r.get(key)
        except RedisError as e:
            return False, None

        if blob is None:
            return False, None
        try:
            return True, pickle.loads(blob)
        except Exception:
            return False, None

    def set(self, key: str, value: Any, ttl: float, *, tags: Optional[Iterable[str]] = None) -> None:
        if ttl is None or ttl <= 0:
            raise ValueError("ttl must be a positive number (seconds)")

        blob = pickle.dumps(value, protocol=self.pickle_protocol)

        ex = None
        px = None
        if ttl is not None:
            if ttl >= 1:
                ex = int(round(ttl))
            else:
                px = int(round(ttl * 1000))

        kt_key = self._ktags_key(key)

        p = self.r.pipeline(transaction=False)
        p.set(name=key, value=blob, ex=ex, px=px)

        read_old = tags is not None
        if read_old:
            p.smembers(kt_key)
        res = p.execute()

        old_tags: set[str] = set()
        if read_old and len(res) >= 2 and isinstance(res[1], set):
            old_tags = {b.decode("utf-8") for b in res[1]} if res[1] else set()

        if tags is None:
            return

        if old_tags:
            for t in old_tags:
                tagset = self._tagset_key(t)
                p = self.r.pipeline(transaction=False)
                p.srem(tagset, key)
                p.scard(tagset)
                scard_res = p.execute()
                if len(scard_res) == 2 and isinstance(scard_res[1], int) and scard_res[1] == 0:
                    try:
                        self.r.delete(tagset)
                    except RedisError:
                        pass

            try:
                self.r.delete(kt_key)
            except RedisError:
                pass

        tset = set(tags)
        if tset:
            p = self.r.pipeline(transaction=False)
            for t in tset:
                p.sadd(self._tagset_key(t), key)
            p.sadd(kt_key, *list(tset))
            if ex is not None:
                p.expire(kt_key, ex)
            elif px is not None:
                p.pexpire(kt_key, px)
            p.execute()
        else:
            pass

    def delete(self, key: str) -> None:
        try:
            kt_key = self._ktags_key(key)
            tags = self.r.smembers(kt_key)
            p = self.r.pipeline(transaction=False)
            p.delete(key)
            if tags:
                for t in (b.decode("utf-8") for b in tags):
                    p.srem(self._tagset_key(t), key)
                p.delete(kt_key)
            p.execute()
        except RedisError:
            return

    def clear(self) -> None:
        try:
            self._delete_by_pattern(f"{self.value_prefix}*")
            self._delete_by_pattern(f"{self.meta_prefix}:*")
        except RedisError:
            return

    def _delete_by_pattern(self, pattern: str) -> None:
        cur = 0
        while True:
            cur, keys = self.r.scan(cursor=cur, match=pattern, count=1000)
            if keys:
                self.r.delete(*keys)
            if cur == 0:
                break

    def invalidate_tags(self, tags: Iterable[str]) -> None:
        tags = list(tags)
        if not tags:
            return
        try:
            tag_keys = [self._tagset_key(t) for t in tags]
            keys: Set[str] = set()
            union = self.r.sunion(tag_keys)
            if union:
                keys = {b.decode("utf-8") for b in union}

            if not keys:
                return

            p = self.r.pipeline(transaction=False)
            for k in keys:
                p.delete(k)
                kt_key = self._ktags_key(k)
                p.smembers(kt_key)
            res = p.execute()

            p = self.r.pipeline(transaction=False)
            idx = 0
            for k in keys:
                kt_key = self._ktags_key(k)
                tags_for_key = self.r.smembers(kt_key)
                if tags_for_key:
                    for t in (b.decode("utf-8") for b in tags_for_key):
                        p.srem(self._tagset_key(t), k)
                p.delete(kt_key)
                idx += 1
            p.execute()
        except RedisError:
            return


default_backend = InMemoryCache()
