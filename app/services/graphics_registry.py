from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from ..core.cache import cache_client
from ..core.config import CACHE_TTL_SECONDS

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
_REGISTRY_PATH = DATA_DIR / "graphics_registry.json"
_LAUNCHERS_PATH = DATA_DIR / "launchers.json"

_ALLOWED_APIS = {"dx12", "dx11", "vulkan"}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_graphics_registry() -> dict[str, Any]:
    cache_key = "graphics:registry"
    cached = cache_client.get_json(cache_key)
    if isinstance(cached, dict):
        return cached
    payload = _load_json(_REGISTRY_PATH)
    cache_client.set_json(cache_key, payload, ttl=CACHE_TTL_SECONDS)
    return payload


def get_registry_entry(app_id: str) -> Optional[dict[str, Any]]:
    if not app_id:
        return None
    registry = load_graphics_registry()
    entry = registry.get(str(app_id))
    return entry if isinstance(entry, dict) else None


def load_launcher_defaults() -> dict[str, Any]:
    cache_key = "graphics:launchers"
    cached = cache_client.get_json(cache_key)
    if isinstance(cached, dict):
        return cached
    payload = _load_json(_LAUNCHERS_PATH)
    defaults = payload.get("defaults") if isinstance(payload.get("defaults"), dict) else {}
    cache_client.set_json(cache_key, defaults, ttl=CACHE_TTL_SECONDS)
    return defaults


def normalize_flags(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item for item in value.split() if item]
    return []


def merge_flags(base: Any, extra: Any) -> list[str]:
    merged: list[str] = []
    seen = set()
    for item in normalize_flags(base) + normalize_flags(extra):
        if item not in seen:
            seen.add(item)
            merged.append(item)
    return merged


def normalize_api(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip().lower()
    return cleaned if cleaned in _ALLOWED_APIS else None
