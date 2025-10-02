from redis import Redis

from custom_cache import cache, RedisCache, invalidate

redis_client = Redis(host="localhost", port=6379, db=0, decode_responses=False)
redis_backend = RedisCache(redis_client, value_prefix="rc:", meta_prefix="rcmeta")

redis_backend.clear()
calls = {"n": 0}


@cache(ttl=30, backend=redis_backend, tags=lambda uid: [f"user:{uid}"])
def load_user(uid: int):
    calls["n"] += 1
    return {"id": uid}


load_user(42)
load_user(42)
assert calls["n"] == 1

invalidate(tags=["user:43"], backend=redis_backend)
load_user(42)
load_user(42)
assert calls["n"] == 1
