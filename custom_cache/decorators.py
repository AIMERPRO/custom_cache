from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Optional, Iterable

from .custom_cache import CustomCache, default_backend
from .key_builder import KeyBuilder
from .utils import Singleflight, AsyncSingleflight, resolve_tags

singleflight = Singleflight()
async_singleflight = AsyncSingleflight()


def cache(
        ttl: Optional[float] = None,
        *,
        key: Optional[Callable[..., str]] = None,
        namespace: Optional[str] = None,
        backend: Optional[CustomCache] = None,
        key_builder: Optional[KeyBuilder] = None,
        tags: Optional[Iterable[str] | Callable[..., Iterable[str]]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    store = backend or default_backend
    kb = key_builder or KeyBuilder(prefix='rc')

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def awrapper(*args: Any, **kwargs: Any) -> Any:
                k = key(*args, **kwargs) if key else kb.build(func, args, kwargs, namespace)
                hit, value = store.get(k)
                if hit:
                    return value

                async with (await async_singleflight.for_key(k)):
                    hit2, value2 = store.get(k)
                    if hit2:
                        return value2
                    result = await func(*args, **kwargs)
                    t = resolve_tags(tags, args, kwargs)
                    store.set(k, result, ttl=ttl, tags=t)
                    return result

            return awrapper

        @functools.wraps(func)
        def swrapper(*args: Any, **kwargs: Any) -> Any:
            k = key(*args, **kwargs) if key else kb.build(func, args, kwargs, namespace)
            hit, value = store.get(k)
            if hit:
                return value

            with singleflight.for_key(k):
                hit2, value2 = store.get(k)
                if hit2:
                    return value2
                result = func(*args, **kwargs)
                t = resolve_tags(tags, args, kwargs)
                store.set(k, result, ttl=ttl, tags=t)
                return result

        return swrapper

    return decorator
