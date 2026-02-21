from __future__ import annotations

from typing import Any, Optional
from urllib.parse import quote

import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests import RequestException
import ctypes
import json
import os
from pathlib import Path
from datetime import datetime, timedelta

from ..core.cache import cache_client
from ..core.config import (
    STEAMGRIDDB_API_KEY,
    STEAMGRIDDB_BASE_URL,
    STEAMGRIDDB_CACHE_TTL_SECONDS,
    STEAMGRIDDB_MAX_CONCURRENCY,
    STEAMGRIDDB_REQUEST_TIMEOUT_SECONDS,
)
import threading
from ..db import SessionLocal
from ..models import SteamGridDBCache
from ..services.steam_catalog import get_steam_summary


class SteamGridDBError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


_STEAMGRIDDB_SEMAPHORE = threading.Semaphore(
    STEAMGRIDDB_MAX_CONCURRENCY if STEAMGRIDDB_MAX_CONCURRENCY > 0 else 4
)
_DISK_CACHE_LOCK = threading.Lock()
_DISK_CACHE_LOADED = False
_DISK_CACHE: dict[str, dict[str, Any]] = {}
_STORAGE_ROOT = Path(
    os.getenv("OTOSHI_STORAGE_DIR", Path(__file__).resolve().parents[2] / "storage")
)
_DISK_CACHE_PATH = _STORAGE_ROOT / "steamgriddb_cache.json"

_TITLE_CLEAN_RE = re.compile(r"[\u2122\u00ae\u00a9]")
_TITLE_EDITION_RE = re.compile(
    r"\b(ultimate|deluxe|complete|goty|gold|platinum|definitive|collector|edition|bundle|pack|"
    r"soundtrack|remaster|remastered|enhanced|beta|demo|showcase|trial|prologue|playtest|benchmark|"
    r"open\s*beta|closed\s*beta|technical\s*test|test\s*server)\b.*",
    re.IGNORECASE,
)
_TITLE_DERIVATIVE_RE = re.compile(
    r"\b(demo|showcase|trial|prologue|playtest|benchmark|alpha|beta|open\s*beta|closed\s*beta|"
    r"technical\s*test|test\s*server)\b",
    re.IGNORECASE,
)
_TITLE_MATCH_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


def build_title_variants(title: str) -> list[str]:
    variants: list[str] = []
    raw = title.strip()
    if not raw:
        return variants
    variants.append(raw)
    cleaned = _TITLE_CLEAN_RE.sub("", raw).strip()
    if cleaned and cleaned not in variants:
        variants.append(cleaned)
    split_chars = [":", "-"]
    for char in split_chars:
        if char in cleaned:
            base = cleaned.split(char, 1)[0].strip()
            if base and base not in variants:
                variants.append(base)
            break
    trimmed = _TITLE_EDITION_RE.sub("", cleaned).strip()
    trimmed = trimmed.rstrip("-:").strip()
    if trimmed and trimmed not in variants:
        variants.append(trimmed)
    return variants


def _is_derivative_title(title: Optional[str]) -> bool:
    text = str(title or "").strip()
    if not text:
        return False
    return _TITLE_DERIVATIVE_RE.search(text) is not None


def _normalize_title_for_match(title: Optional[str]) -> str:
    text = str(title or "").strip().lower()
    if not text:
        return ""
    text = _TITLE_CLEAN_RE.sub("", text)
    text = text.replace("’", "'").replace("`", "'").replace("'", "")
    normalized = _TITLE_MATCH_NORMALIZE_RE.sub(" ", text)
    return " ".join(normalized.split()).strip()


def _request(path: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    data = _request_raw(path, params)
    if not data:
        return []
    if isinstance(data, list):
        return data
    return [data]


def _request_raw(path: str, params: Optional[dict[str, Any]] = None) -> Any:
    if not STEAMGRIDDB_API_KEY:
        raise SteamGridDBError("SteamGridDB not configured")
    url = f"{STEAMGRIDDB_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {STEAMGRIDDB_API_KEY}",
        "Accept": "application/json",
        "User-Agent": "otoshi-launcher/1.0",
    }
    attempt = 0
    last_error: Optional[SteamGridDBError] = None
    while attempt < 3:
        attempt += 1
        try:
            with _STEAMGRIDDB_SEMAPHORE:
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=STEAMGRIDDB_REQUEST_TIMEOUT_SECONDS,
                )
        except RequestException:
            last_error = SteamGridDBError("SteamGridDB network error", status_code=503)
            time.sleep(0.4 * attempt)
            continue
        if response.status_code == 429 and attempt < 3:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = float(retry_after) if retry_after else 0.8 * attempt
            time.sleep(wait_seconds)
            continue
        if response.status_code >= 500 and attempt < 3:
            time.sleep(0.6 * attempt)
            continue
        if response.status_code >= 400:
            raise SteamGridDBError(
                f"SteamGridDB error {response.status_code}",
                status_code=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise SteamGridDBError("SteamGridDB invalid response") from exc
        if not payload.get("success", False):
            errors = payload.get("errors") or ["SteamGridDB request failed"]
            raise SteamGridDBError(str(errors[0]))
        return payload.get("data")
    if last_error:
        raise last_error
    raise SteamGridDBError("SteamGridDB request failed")


def _load_disk_cache() -> None:
    global _DISK_CACHE_LOADED
    if _DISK_CACHE_LOADED:
        return
    with _DISK_CACHE_LOCK:
        if _DISK_CACHE_LOADED:
            return
        if _DISK_CACHE_PATH.exists():
            try:
                payload = json.loads(_DISK_CACHE_PATH.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    _DISK_CACHE.update(payload)
            except json.JSONDecodeError:
                pass
        _DISK_CACHE_LOADED = True


def _disk_cache_get(key: str) -> Optional[Any]:
    _load_disk_cache()
    with _DISK_CACHE_LOCK:
        entry = _DISK_CACHE.get(key)
        if not entry:
            return None
        expires_at = entry.get("expires_at")
        if expires_at is not None and expires_at < time.time():
            _DISK_CACHE.pop(key, None)
            return None
        return entry.get("value")


def _disk_cache_set(key: str, value: Any, ttl: int) -> None:
    _load_disk_cache()
    expires_at = time.time() + ttl if ttl else None
    with _DISK_CACHE_LOCK:
        _DISK_CACHE[key] = {"value": value, "expires_at": expires_at}
        try:
            _DISK_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            _DISK_CACHE_PATH.write_text(json.dumps(_DISK_CACHE), encoding="utf-8")
        except OSError:
            pass


def _cache_get_json(key: str) -> Optional[Any]:
    cached = cache_client.get_json(key)
    if cached is not None:
        return cached
    cached = _disk_cache_get(key)
    if cached is not None:
        cache_client.set_json(key, cached, ttl=STEAMGRIDDB_CACHE_TTL_SECONDS)
    return cached


def _cache_set_json(key: str, value: Any, ttl: int) -> None:
    cache_client.set_json(key, value, ttl=ttl)
    _disk_cache_set(key, value, ttl=ttl)


def _pick_best(items: list[dict[str, Any]]) -> Optional[str]:
    if not items:
        return None

    def score(item: dict[str, Any]) -> tuple[int, int]:
        width = int(item.get("width") or 0)
        height = int(item.get("height") or 0)
        return (int(item.get("score") or 0), width * height)

    best = max(items, key=score)
    return best.get("url") or best.get("url_thumb") or best.get("thumb")


def search_game_by_title(title: str) -> Optional[dict[str, Any]]:
    cache_key = f"steamgriddb:search:v2:{title.lower()}"
    cached = _cache_get_json(cache_key)
    if cached:
        return cached if cached.get("id") else None
    try:
        data = _request(f"search/autocomplete/{quote(title)}")
    except SteamGridDBError as exc:
        if exc.status_code == 404:
            _cache_set_json(
                cache_key, {"id": None, "name": title}, ttl=STEAMGRIDDB_CACHE_TTL_SECONDS
            )
            return None
        raise
    if not data:
        _cache_set_json(cache_key, {"id": None, "name": title}, ttl=STEAMGRIDDB_CACHE_TTL_SECONDS)
        return None
    exact = next((item for item in data if (item.get("name") or "").lower() == title.lower()), None)
    candidate = exact
    if candidate is None:
        title_norm = _normalize_title_for_match(title)
        if title_norm:
            candidate = next(
                (
                    item
                    for item in data
                    if _normalize_title_for_match(item.get("name") or "") == title_norm
                ),
                None,
            )
    if candidate is None:
        _cache_set_json(cache_key, {"id": None, "name": title}, ttl=STEAMGRIDDB_CACHE_TTL_SECONDS)
        return None
    result = {
        "id": candidate.get("id"),
        "name": candidate.get("name") or title,
    }
    _cache_set_json(cache_key, result, ttl=STEAMGRIDDB_CACHE_TTL_SECONDS)
    return result


def search_game_by_steam_id(steam_app_id: str) -> Optional[dict[str, Any]]:
    cache_key = f"steamgriddb:steam:{steam_app_id}"
    cached = _cache_get_json(cache_key)
    if cached:
        return cached if cached.get("id") else None
    try:
        data = _request_raw(f"games/steam/{quote(steam_app_id)}")
    except SteamGridDBError as exc:
        if exc.status_code == 404:
            _cache_set_json(
                cache_key,
                {"id": None, "name": steam_app_id},
                ttl=STEAMGRIDDB_CACHE_TTL_SECONDS,
            )
            return None
        raise
    if not data or not isinstance(data, dict):
        _cache_set_json(
            cache_key,
            {"id": None, "name": steam_app_id},
            ttl=STEAMGRIDDB_CACHE_TTL_SECONDS,
        )
        return None
    result = {
        "id": data.get("id"),
        "name": data.get("name") or steam_app_id,
    }
    _cache_set_json(cache_key, result, ttl=STEAMGRIDDB_CACHE_TTL_SECONDS)
    return result


_native_steam_fallback = None


def _load_native_fallback():
    global _native_steam_fallback
    if _native_steam_fallback is not None:
        return _native_steam_fallback
    lib_path = os.getenv("LAUNCHER_CORE_PATH", "")
    if not lib_path:
        _native_steam_fallback = None
        return None
    try:
        lib = ctypes.CDLL(lib_path)
    except OSError:
        _native_steam_fallback = None
        return None
    try:
        func = lib.launcher_build_steam_fallback
        func.argtypes = [ctypes.c_uint64, ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t]
        func.restype = ctypes.c_int
        _native_steam_fallback = func
        return func
    except AttributeError:
        _native_steam_fallback = None
        return None


def build_steam_fallback_assets(steam_app_id: str) -> dict[str, Optional[str]]:
    if not steam_app_id or not str(steam_app_id).isdigit():
        return {"grid": None, "hero": None, "logo": None, "icon": None}
    native = _load_native_fallback()
    if native:
        buffer = (ctypes.c_ubyte * 512)()
        result = native(int(steam_app_id), buffer, ctypes.sizeof(buffer))
        if result == 0:
            try:
                raw = bytes(buffer).split(b"\0", 1)[0].decode("utf-8")
                payload = json.loads(raw)
                return {
                    "grid": payload.get("grid"),
                    "hero": payload.get("hero"),
                    "logo": payload.get("logo"),
                    "icon": payload.get("icon"),
                }
            except Exception:
                pass
    base = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{steam_app_id}"
    return {
        "grid": f"{base}/library_600x900.jpg",
        "hero": f"{base}/library_hero.jpg",
        "logo": f"{base}/logo.png",
        "icon": f"{base}/icon.jpg",
    }


def _normalize_assets_payload(
    steam_app_id: str,
    title: Optional[str],
    assets: Optional[dict[str, Optional[str]]] = None,
) -> dict[str, Optional[str]]:
    fallback = build_steam_fallback_assets(steam_app_id)
    grid = (assets or {}).get("grid") or fallback.get("grid") or fallback.get("hero")
    hero = (assets or {}).get("hero") or fallback.get("hero") or grid
    logo = (assets or {}).get("logo") or (assets or {}).get("icon") or fallback.get("logo")
    raw_icon = (assets or {}).get("icon")
    if raw_icon and steam_app_id:
        marker = f"/steam/apps/{steam_app_id}/icon.jpg"
        lower_icon = raw_icon.lower()
        if marker in lower_icon and any(
            value and "steamgriddb.com" in value.lower()
            for value in (logo, grid, hero)
        ):
            raw_icon = None
    icon = (
        raw_icon
        or (assets or {}).get("logo")
        or grid
        or hero
        or fallback.get("icon")
        or fallback.get("logo")
    )
    merged = {
        "grid": grid,
        "hero": hero,
        "logo": logo,
        "icon": icon,
    }
    return {
        "game_id": int((assets or {}).get("game_id") or 0),
        "name": title or steam_app_id,
        "grid": merged.get("grid"),
        "hero": merged.get("hero"),
        "logo": merged.get("logo"),
        "icon": merged.get("icon"),
    }


def get_cached_assets(
    steam_app_id: str,
    *,
    prefer_steamgriddb: bool = False,
) -> Optional[dict[str, Optional[str]]]:
    if not steam_app_id or not str(steam_app_id).isdigit():
        return None
    try:
        with SessionLocal() as db:
            entry = (
                db.query(SteamGridDBCache)
                .filter(SteamGridDBCache.steam_app_id == steam_app_id)
                .first()
            )
            if not entry:
                return None
            if entry.expires_at and entry.expires_at < datetime.utcnow():
                return None
            if (
                prefer_steamgriddb
                and STEAMGRIDDB_API_KEY
                and (entry.source or "").strip().lower() == "steam_fallback"
            ):
                return None
            payload = _normalize_assets_payload(
                steam_app_id=steam_app_id,
                title=entry.title or steam_app_id,
                assets={
                    "game_id": entry.sgdb_game_id or 0,
                    "grid": entry.grid_url,
                    "hero": entry.hero_url,
                    "logo": entry.logo_url,
                    "icon": entry.icon_url,
                },
            )
            payload["game_id"] = entry.sgdb_game_id or 0
            return payload
    except Exception:
        return None


def save_cached_assets(
    steam_app_id: str,
    title: str,
    sgdb_game_id: Optional[int],
    assets: dict[str, Optional[str]],
    source: str,
) -> None:
    if not steam_app_id or not str(steam_app_id).isdigit():
        return
    now = datetime.utcnow()
    expires_at = now + timedelta(seconds=STEAMGRIDDB_CACHE_TTL_SECONDS)
    sgdb_game_id = sgdb_game_id if sgdb_game_id and sgdb_game_id > 0 else None
    try:
        with SessionLocal() as db:
            entry = (
                db.query(SteamGridDBCache)
                .filter(SteamGridDBCache.steam_app_id == steam_app_id)
                .first()
            )
            if entry is None:
                entry = SteamGridDBCache(
                    steam_app_id=steam_app_id,
                    title=title or None,
                    sgdb_game_id=sgdb_game_id,
                    grid_url=assets.get("grid"),
                    hero_url=assets.get("hero"),
                    logo_url=assets.get("logo"),
                    icon_url=assets.get("icon"),
                    source=source,
                    fetched_at=now,
                    expires_at=expires_at,
                )
                db.add(entry)
            else:
                entry.title = title or entry.title
                entry.sgdb_game_id = sgdb_game_id
                entry.grid_url = assets.get("grid")
                entry.hero_url = assets.get("hero")
                entry.logo_url = assets.get("logo")
                entry.icon_url = assets.get("icon")
                entry.source = source
                entry.fetched_at = now
                entry.expires_at = expires_at
            db.commit()
    except Exception:
        return


def resolve_assets(steam_app_id: Optional[str], title: Optional[str]) -> dict[str, Optional[str]]:
    steam_app_id = str(steam_app_id or "")
    if steam_app_id:
        cached = get_cached_assets(steam_app_id, prefer_steamgriddb=True)
        if cached:
            return _normalize_assets_payload(
                steam_app_id=steam_app_id,
                title=title or str(cached.get("name") or steam_app_id),
                assets=cached,
            )

    search_title = title
    if not search_title and steam_app_id:
        summary = get_steam_summary(steam_app_id)
        search_title = summary.get("name") if summary else None

    game = None
    try:
        derivative_title = _is_derivative_title(search_title)
        if search_title and derivative_title:
            for candidate in build_title_variants(search_title):
                try:
                    game = search_game_by_title(candidate)
                except SteamGridDBError:
                    game = None
                if game:
                    break
        if not game and steam_app_id:
            try:
                game = search_game_by_steam_id(steam_app_id)
            except SteamGridDBError:
                game = None
        if not game and search_title and not derivative_title:
            for candidate in build_title_variants(search_title):
                try:
                    game = search_game_by_title(candidate)
                except SteamGridDBError:
                    game = None
                if game:
                    break
    except SteamGridDBError:
        game = None

    if not game or not game.get("id"):
        fallback = build_steam_fallback_assets(steam_app_id)
        result = _normalize_assets_payload(
            steam_app_id=steam_app_id,
            title=search_title or steam_app_id,
            assets=fallback,
        )
        save_cached_assets(
            steam_app_id,
            str(result["name"]),
            None,
            fallback,
            source="steam_fallback",
        )
        return result

    assets = fetch_assets(int(game["id"]))
    fallback = build_steam_fallback_assets(steam_app_id)
    grid = assets.get("grid") or fallback.get("grid") or fallback.get("hero")
    hero = assets.get("hero") or fallback.get("hero") or grid
    logo = assets.get("logo") or assets.get("icon") or fallback.get("logo")
    raw_icon = assets.get("icon")
    if raw_icon and steam_app_id:
        marker = f"/steam/apps/{steam_app_id}/icon.jpg"
        lower_icon = raw_icon.lower()
        if marker in lower_icon and any(
            value and "steamgriddb.com" in value.lower()
            for value in (logo, grid, hero)
        ):
            raw_icon = None
    icon = (
        raw_icon
        or assets.get("logo")
        or grid
        or hero
        or fallback.get("icon")
        or fallback.get("logo")
    )
    merged = {
        "grid": grid,
        "hero": hero,
        "logo": logo,
        "icon": icon,
    }
    result = {
        "game_id": int(game["id"]),
        "name": game.get("name") or search_title or steam_app_id,
        "grid": merged.get("grid"),
        "hero": merged.get("hero"),
        "logo": merged.get("logo"),
        "icon": merged.get("icon"),
    }
    source = "steamgriddb" if any(assets.values()) else "steam_fallback"
    save_cached_assets(
        steam_app_id,
        result["name"],
        int(game["id"]),
        merged,
        source=source,
    )
    return result


def prewarm_steamgriddb_cache(appids: list[str], concurrency: int) -> None:
    if not STEAMGRIDDB_API_KEY:
        return
    appids = [str(app_id) for app_id in appids if str(app_id).isdigit()]
    if not appids:
        return
    max_workers = max(1, concurrency)

    def _worker(app_id: str) -> None:
        cached = get_cached_assets(app_id, prefer_steamgriddb=True)
        if cached:
            return
        title = None
        summary = get_steam_summary(app_id)
        if summary:
            title = summary.get("name")
        resolve_assets(app_id, title)
        time.sleep(0.15)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_worker, app_id) for app_id in appids]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                continue


def fetch_assets(game_id: int) -> dict[str, Optional[str]]:
    cache_key = f"steamgriddb:assets:{game_id}"
    cached = _cache_get_json(cache_key)
    if cached:
        return cached

    def safe_pick(path: str) -> Optional[str]:
        try:
            return _pick_best(_request(path))
        except SteamGridDBError:
            return None

    assets = {
        "grid": safe_pick(f"grids/game/{game_id}"),
        "hero": safe_pick(f"heroes/game/{game_id}"),
        "logo": safe_pick(f"logos/game/{game_id}"),
        "icon": safe_pick(f"icons/game/{game_id}"),
    }
    _cache_set_json(cache_key, assets, ttl=STEAMGRIDDB_CACHE_TTL_SECONDS)
    return assets
