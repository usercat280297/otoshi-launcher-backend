from __future__ import annotations

import json
import locale as pylocale
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional
import ctypes

from ..core.config import LAUNCHER_CORE_PATH, SETTINGS_STORAGE_PATH

_LOCK = threading.Lock()
_CACHE: Optional[dict] = None
SUPPORTED_LOCALES = {"en", "vi"}
SEARCH_HISTORY_LIMIT = 12


def _load_settings() -> dict:
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    with _LOCK:
        if _CACHE is not None:
            return _CACHE
        path = Path(SETTINGS_STORAGE_PATH)
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    _CACHE = payload
                    return _CACHE
            except json.JSONDecodeError:
                pass
        _CACHE = {}
        return _CACHE


def _write_settings(payload: dict) -> None:
    path = Path(SETTINGS_STORAGE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = datetime.utcnow().isoformat()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


_native_locale_func = None


def _load_native_locale_func():
    global _native_locale_func
    if _native_locale_func is not None:
        return _native_locale_func
    if not LAUNCHER_CORE_PATH:
        _native_locale_func = None
        return None
    try:
        lib = ctypes.CDLL(LAUNCHER_CORE_PATH)
    except OSError:
        _native_locale_func = None
        return None
    try:
        func = lib.launcher_get_system_locale
        func.argtypes = [ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t]
        func.restype = ctypes.c_int
        _native_locale_func = func
        return func
    except AttributeError:
        _native_locale_func = None
        return None


def _native_system_locale() -> Optional[str]:
    func = _load_native_locale_func()
    if not func:
        return None
    buffer = (ctypes.c_ubyte * 128)()
    if func(buffer, ctypes.sizeof(buffer)) != 0:
        return None
    raw = bytes(buffer).split(b"\0", 1)[0].decode("utf-8", errors="ignore").strip()
    return raw or None


def detect_system_locale() -> Optional[str]:
    native_value = _native_system_locale()
    if native_value:
        return native_value
    try:
        value = pylocale.getdefaultlocale()[0] or pylocale.getlocale()[0]
    except Exception:
        value = None
    if not value:
        value = os.getenv("LANG") or os.getenv("LC_ALL") or os.getenv("LC_MESSAGES")
    return value or None


def normalize_locale(value: Optional[str]) -> str:
    if not value:
        return "en"
    cleaned = value.replace("_", "-").lower()
    if cleaned.startswith("vi"):
        return "vi"
    return "en"


def get_user_locale() -> Optional[str]:
    payload = _load_settings()
    value = payload.get("locale")
    if value in SUPPORTED_LOCALES:
        return value
    return None


def set_user_locale(locale: str) -> str:
    normalized = normalize_locale(locale)
    if normalized not in SUPPORTED_LOCALES:
        normalized = "en"
    with _LOCK:
        payload = _load_settings()
        payload["locale"] = normalized
        _write_settings(payload)
    return normalized


def _normalize_search_query(value: str) -> str:
    return " ".join(value.strip().split())


def get_search_history(limit: int = SEARCH_HISTORY_LIMIT) -> list[dict]:
    payload = _load_settings()
    items = payload.get("search_history") or []
    cleaned = []
    for item in items:
        if not isinstance(item, dict):
            continue
        query = item.get("query")
        if not isinstance(query, str) or not query.strip():
            continue
        cleaned.append(
            {
                "query": query.strip(),
                "count": int(item.get("count") or 1),
                "last_used": item.get("last_used"),
            }
        )
    cleaned.sort(key=lambda entry: entry.get("last_used") or "", reverse=True)
    return cleaned[: max(1, limit)]


def add_search_history(query: str, limit: int = SEARCH_HISTORY_LIMIT) -> list[dict]:
    normalized = _normalize_search_query(query)
    if not normalized:
        return get_search_history(limit)
    now = datetime.utcnow().isoformat()
    with _LOCK:
        payload = _load_settings()
        items = payload.get("search_history") or []
        updated = []
        matched = False
        for item in items:
            if not isinstance(item, dict):
                continue
            stored_query = item.get("query")
            if not isinstance(stored_query, str):
                continue
            stored_norm = _normalize_search_query(stored_query).lower()
            if stored_norm == normalized.lower():
                updated.append(
                    {
                        "query": stored_query.strip(),
                        "count": int(item.get("count") or 1) + 1,
                        "last_used": now,
                    }
                )
                matched = True
            else:
                updated.append(
                    {
                        "query": stored_query.strip(),
                        "count": int(item.get("count") or 1),
                        "last_used": item.get("last_used"),
                    }
                )
        if not matched:
            updated.append({"query": normalized, "count": 1, "last_used": now})
        updated.sort(key=lambda entry: entry.get("last_used") or "", reverse=True)
        payload["search_history"] = updated[: max(1, limit)]
        payload["last_search"] = normalized
        _write_settings(payload)
    return get_search_history(limit)


def clear_search_history() -> None:
    with _LOCK:
        payload = _load_settings()
        payload["search_history"] = []
        _write_settings(payload)


def get_download_settings() -> dict:
    payload = _load_settings()
    value = payload.get("download_settings")
    return value if isinstance(value, dict) else {}


def set_download_settings(updates: dict) -> dict:
    cleaned = updates if isinstance(updates, dict) else {}
    with _LOCK:
        payload = _load_settings()
        current = payload.get("download_settings")
        if not isinstance(current, dict):
            current = {}
        current.update(cleaned)
        payload["download_settings"] = current
        _write_settings(payload)
    return current
