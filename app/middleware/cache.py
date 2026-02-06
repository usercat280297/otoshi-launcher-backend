from functools import wraps
import hashlib
import json
from typing import Callable

from ..core.cache import cache_client


def cache_response(ttl: int = 300):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key_source = json.dumps(kwargs, sort_keys=True, default=str)
            cache_key = f"{func.__name__}:{hashlib.md5(key_source.encode('utf-8')).hexdigest()}"
            cached = cache_client.get_json(cache_key)
            if cached is not None:
                return cached

            result = func(*args, **kwargs)
            cache_client.set_json(cache_key, result, ttl=ttl)
            return result

        return wrapper

    return decorator
