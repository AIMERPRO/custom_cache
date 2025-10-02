from __future__ import annotations
import hashlib
import pickle
from typing import Any, Callable, Optional, Tuple


class KeyBuilder:
    """
    Строит ключи вида:
    <prefix>:<namespace(optional)>:<module>.<qualname>:<sha256(args, kwargs)>
    """
    def __init__(
        self,
        *,
        prefix: str = "rc",
        namespace: Optional[str] = None,
        hash_factory: Callable[[], "hashlib._Hash"] = hashlib.sha256,
    ) -> None:
        self.prefix = prefix.rstrip(":")
        self.namespace = namespace
        self.hash_factory = hash_factory

    @staticmethod
    def _func_base(func: Callable[..., Any]) -> str:
        qualname = getattr(func, "__qualname__", getattr(func, "__name__", "func"))
        return f"{func.__module__}.{qualname}"

    @staticmethod
    def _payload(args: Tuple[Any, ...], kwargs: dict) -> bytes:
        try:
            return pickle.dumps((args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            return repr((args, kwargs)).encode("utf-8")

    def build(
        self,
        func: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: dict,
        namespace: Optional[str] = None,
    ) -> str:
        base = self._func_base(func)
        ns = namespace or self.namespace
        if ns:
            base = f"{ns}:{base}"

        h = self.hash_factory()
        h.update(self._payload(args, kwargs))
        digest = h.hexdigest()
        return f"{self.prefix}:{base}:{digest}"