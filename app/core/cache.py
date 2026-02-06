import json
import time
from pathlib import Path
from typing import Any, Optional

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None

from .config import REDIS_URL, CACHE_TTL_SECONDS

# File-based storage for OAuth states to survive restarts
_OAUTH_STATE_FILE = Path(__file__).resolve().parents[2] / "storage" / "oauth_states.json"


def _load_oauth_states() -> dict[str, tuple[Optional[float], str]]:
    """Load OAuth states from file."""
    if not _OAUTH_STATE_FILE.exists():
        return {}
    try:
        data = json.loads(_OAUTH_STATE_FILE.read_text(encoding="utf-8"))
        # Clean expired states
        now = time.time()
        return {
            k: (v[0], v[1])
            for k, v in data.items()
            if v[0] is None or v[0] > now
        }
    except Exception:
        return {}


def _save_oauth_states(states: dict[str, tuple[Optional[float], str]]) -> None:
    """Save OAuth states to file."""
    try:
        _OAUTH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Clean expired before saving
        now = time.time()
        cleaned = {
            k: v for k, v in states.items()
            if v[0] is None or v[0] > now
        }
        print(f"[OAuth File DEBUG] Saving {len(cleaned)} states to file: {list(cleaned.keys())}")
        _OAUTH_STATE_FILE.write_text(json.dumps(cleaned), encoding="utf-8")
        # Verify write
        verify = json.loads(_OAUTH_STATE_FILE.read_text(encoding="utf-8"))
        print(f"[OAuth File DEBUG] File saved, verification read: {len(verify)} states")
    except Exception as e:
        print(f"[OAuth File ERROR] Failed to save states: {e}")


class CacheClient:
    def __init__(self) -> None:
        self.redis: Optional["redis.Redis"] = None
        self.fallback: dict[str, tuple[Optional[float], str]] = {}
        self.sessions: dict[str, tuple[str, Optional[float]]] = {}
        self.rate_limits: dict[str, tuple[int, Optional[float]]] = {}
        # Load persisted OAuth states on init
        self._oauth_states: dict[str, tuple[Optional[float], str]] = _load_oauth_states()

    def connect(self) -> None:
        if not REDIS_URL or redis is None:
            return
        try:
            client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            client.ping()
            self.redis = client
        except Exception:
            self.redis = None

    def disconnect(self) -> None:
        if not self.redis:
            return
        try:
            self.redis.close()
        finally:
            self.redis = None

    def get_json(self, key: str) -> Optional[Any]:
        raw = self.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def set_json(self, key: str, value: Any, ttl: int = CACHE_TTL_SECONDS) -> None:
        self.set(key, json.dumps(value), ttl)

    def get(self, key: str) -> Optional[str]:
        if self.redis:
            return self.redis.get(key)
        entry = self.fallback.get(key)
        if not entry:
            return None
        expires_at, payload = entry
        if expires_at is not None and expires_at < time.time():
            del self.fallback[key]
            return None
        return payload

    def set(self, key: str, value: str, ttl: int = CACHE_TTL_SECONDS) -> None:
        if self.redis:
            self.redis.setex(key, ttl, value)
            return
        expires_at = time.time() + ttl if ttl else None
        self.fallback[key] = (expires_at, value)

    def delete(self, key: str) -> None:
        if self.redis:
            self.redis.delete(key)
            return
        self.fallback.pop(key, None)

    def set_session(self, user_id: str, token: str, ttl: int) -> None:
        key = f"session:{user_id}"
        if self.redis:
            self.redis.setex(key, ttl, token)
            return
        expires_at = time.time() + ttl if ttl else None
        self.sessions[key] = (token, expires_at)

    def get_session(self, user_id: str) -> Optional[str]:
        key = f"session:{user_id}"
        if self.redis:
            return self.redis.get(key)
        entry = self.sessions.get(key)
        if not entry:
            return None
        token, expires_at = entry
        if expires_at is not None and expires_at < time.time():
            del self.sessions[key]
            return None
        return token

    def delete_session(self, user_id: str) -> None:
        key = f"session:{user_id}"
        if self.redis:
            self.redis.delete(key)
            return
        self.sessions.pop(key, None)

    def check_rate_limit(self, key: str, limit: int, window_seconds: int) -> bool:
        full_key = f"ratelimit:{key}"
        if self.redis:
            current = self.redis.incr(full_key)
            if current == 1:
                self.redis.expire(full_key, window_seconds)
            return current <= limit

        now = time.time()
        current, expires_at = self.rate_limits.get(full_key, (0, None))
        if expires_at is None or expires_at < now:
            current = 0
            expires_at = now + window_seconds
        current += 1
        self.rate_limits[full_key] = (current, expires_at)
        return current <= limit

    # --- OAuth state methods with file persistence ---
    def get_oauth_state(self, key: str) -> Optional[Any]:
        """Get OAuth state with file-based persistence."""
        if self.redis:
            raw = self.redis.get(key)
            if raw:
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return None
            return None
        # Always reload from file to get latest states (survives reload)
        self._oauth_states = _load_oauth_states()
        print(f"[OAuth Cache DEBUG] Loaded states from file: {list(self._oauth_states.keys())}")
        entry = self._oauth_states.get(key)
        if not entry:
            print(f"[OAuth Cache DEBUG] Key {key} not found in states")
            return None
        expires_at, payload = entry
        if expires_at is not None and expires_at < time.time():
            print(f"[OAuth Cache DEBUG] Key {key} expired")
            del self._oauth_states[key]
            _save_oauth_states(self._oauth_states)
            return None
        try:
            result = json.loads(payload)
            print(f"[OAuth Cache DEBUG] Key {key} found: {result}")
            return result
        except json.JSONDecodeError:
            return None

    def set_oauth_state(self, key: str, value: Any, ttl: int) -> None:
        """Set OAuth state with file-based persistence."""
        if self.redis:
            self.redis.setex(key, ttl, json.dumps(value))
            return
        # Reload first to not lose other states
        self._oauth_states = _load_oauth_states()
        expires_at = time.time() + ttl if ttl else None
        self._oauth_states[key] = (expires_at, json.dumps(value))
        _save_oauth_states(self._oauth_states)
        print(f"[OAuth Cache DEBUG] Saved state {key}, file now has: {list(self._oauth_states.keys())}")

    def delete_oauth_state(self, key: str) -> None:
        """Delete OAuth state."""
        if self.redis:
            self.redis.delete(key)
            return
        self._oauth_states.pop(key, None)
        _save_oauth_states(self._oauth_states)


cache_client = CacheClient()
