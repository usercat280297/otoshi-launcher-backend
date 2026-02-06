from __future__ import annotations

import html
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Optional, Any, Dict, List
import ctypes
import os

import requests
import bleach

from ..core.cache import cache_client
from ..core.denuvo import DENUVO_APP_IDS, DENUVO_APP_ID_SET
from ..core.config import (
    LUA_FILES_DIR,
    LUA_REMOTE_ONLY,
    STEAM_CACHE_TTL_SECONDS,
    STEAM_CATALOG_CACHE_TTL_SECONDS,
    STEAM_REQUEST_TIMEOUT_SECONDS,
    STEAM_STORE_API_URL,
    STEAM_STORE_SEARCH_URL,
    STEAM_TRENDING_CACHE_TTL_SECONDS,
    STEAM_TRENDING_LIMIT,
    STEAM_WEB_API_KEY,
    STEAM_WEB_API_URL,
)
from ..services.remote_game_data import get_lua_appids_from_server

TAG_RE = re.compile(r"<[^>]+>")
MEDIA_VERSION = 6

STEAM_HTML_TAGS = [
    "a",
    "abbr",
    "b",
    "blockquote",
    "br",
    "code",
    "div",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "img",
    "li",
    "ol",
    "p",
    "pre",
    "span",
    "strong",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "u",
    "ul",
    "video",
    "source",
]
STEAM_HTML_ATTRIBUTES = {
    "*": ["class"],
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "title", "width", "height", "loading"],
    "video": [
        "src",
        "poster",
        "width",
        "height",
        "autoplay",
        "muted",
        "loop",
        "playsinline",
        "controls",
        "preload",
    ],
    "source": ["src", "type"],
    "td": ["colspan", "rowspan", "align"],
    "th": ["colspan", "rowspan", "align"],
}
STEAM_HTML_PROTOCOLS = ["http", "https"]

_NATIVE_MOVIE_BUILDER = None


_native_movie_builder: Optional[Any] = None


def _load_native_movie_builder():
    global _native_movie_builder
    if _native_movie_builder is not None:
        return _native_movie_builder
    lib_path = os.getenv("LAUNCHER_CORE_PATH", "")
    if not lib_path:
        _native_movie_builder = None
        return None
    try:
        lib = ctypes.CDLL(lib_path)
    except OSError:
        _native_movie_builder = None
        return None
    try:
        func = lib.launcher_build_steam_movie_url
        func.argtypes = [ctypes.c_uint64, ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t]
        func.restype = ctypes.c_int
        _native_movie_builder = func
        return func
    except AttributeError:
        _native_movie_builder = None
        return None


def get_hot_appids() -> List[str]:
    cache_key = "steam:hot_appids"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached
    if not STEAM_WEB_API_KEY:
        return []
    url = f"{STEAM_WEB_API_URL.rstrip('/')}/ISteamChartsService/GetMostPlayedGames/v1/"
    payload = _request(url, {"key": STEAM_WEB_API_KEY})
    response = payload.get("response") if payload else None
    ranks = response.get("ranks") if response else None
    if not ranks or not isinstance(ranks, list):
        return []
    appids = [str(item.get("appid")) for item in ranks if isinstance(item, dict) and item.get("appid")]
    if STEAM_TRENDING_LIMIT > 0:
        appids = appids[:STEAM_TRENDING_LIMIT]
    cache_client.set_json(cache_key, appids, ttl=STEAM_TRENDING_CACHE_TTL_SECONDS)
    return appids


def prioritize_appids(appids: List[str]) -> List[str]:
    seen: set[str] = set()
    prioritized: list[str] = []
    appid_set = set(appids)
    hot_ids = get_hot_appids()
    for app_id in DENUVO_APP_IDS:
        app_str = str(app_id)
        if app_str in appid_set and app_str not in seen:
            prioritized.append(app_str)
            seen.add(app_str)
    for app_id in hot_ids:
        if app_id in appid_set and app_id not in seen:
            prioritized.append(app_id)
            seen.add(app_id)
    for app_id in appids:
        if app_id not in seen:
            prioritized.append(app_id)
            seen.add(app_id)
    return prioritized


def prioritize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    denuvo_items = []
    hot_items = []
    other_items = []
    hot_set = set(get_hot_appids())
    for item in items:
        app_id = str(item.get("app_id") or "")
        if app_id in DENUVO_APP_ID_SET:
            denuvo_items.append(item)
        elif app_id in hot_set:
            hot_items.append(item)
        else:
            other_items.append(item)
    return denuvo_items + hot_items + other_items


def _strip_html(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    text = re.sub(r"</p>|</li>|</div>", "\n", text)
    text = TAG_RE.sub("", text)
    text = html.unescape(text)
    return text.strip()


def _sanitize_html(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = bleach.clean(
        value,
        tags=STEAM_HTML_TAGS,
        attributes=STEAM_HTML_ATTRIBUTES,
        protocols=STEAM_HTML_PROTOCOLS,
        strip=True,
        strip_comments=True,
    )
    cleaned = cleaned.replace("\r\n", "\n").strip()
    return cleaned or None


def _request(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(
            url,
            params=params,
            timeout=STEAM_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "otoshi-launcher/1.0"},
        )
        if response.status_code != 200:
            return None
        return response.json()
    except (requests.RequestException, ValueError):
        return None


def _store_appdetails(appids: Iterable[str], filters: Optional[str] = None) -> Dict[str, Any]:
    url = f"{STEAM_STORE_API_URL.rstrip('/')}/appdetails"
    params = {
        "appids": ",".join(appids),
        "cc": "us",
        "l": "en",
    }
    if filters:
        params["filters"] = filters
    payload = _request(url, params)
    return payload or {}


def _parse_price(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    price = payload.get("price_overview") or {}
    if payload.get("is_free"):
        return {
            "initial": 0,
            "final": 0,
            "discount_percent": 0,
            "currency": price.get("currency"),
            "formatted": "Free",
            "final_formatted": "Free",
        }
    if not price:
        return None
    return {
        "initial": price.get("initial"),
        "final": price.get("final"),
        "discount_percent": price.get("discount_percent"),
        "currency": price.get("currency"),
        "formatted": price.get("initial_formatted"),
        "final_formatted": price.get("final_formatted"),
    }


def _parse_platforms(payload: Dict[str, Any]) -> List[str]:
    platforms = payload.get("platforms") or {}
    return [key for key, enabled in platforms.items() if enabled]


def _parse_required_age(payload: Dict[str, Any]) -> Optional[int]:
    value = payload.get("required_age")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_genres(payload: Dict[str, Any]) -> List[str]:
    return [item.get("description") for item in (payload.get("genres") or []) if item.get("description")]


def _parse_categories(payload: Dict[str, Any]) -> List[str]:
    return [item.get("description") for item in (payload.get("categories") or []) if item.get("description")]


def _parse_screenshots(payload: Dict[str, Any]) -> List[str]:
    shots = [shot.get("path_full") for shot in (payload.get("screenshots") or []) if shot.get("path_full")]
    if shots:
        return shots
    fallback = []
    for key in ("header_image", "capsule_image", "background", "background_raw"):
        value = payload.get(key)
        if value:
            fallback.append(value)
    return fallback


def _build_movie_fallback_url(movie_id: Optional[int]) -> Optional[str]:
    if not movie_id:
        return None
    native = _load_native_movie_builder()
    if native:
        buffer = (ctypes.c_ubyte * 256)()
        result = native(movie_id, buffer, ctypes.sizeof(buffer))
        if result == 0:
            try:
                return bytes(buffer).split(b"\0", 1)[0].decode("utf-8")
            except (UnicodeDecodeError, ValueError):
                pass
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{movie_id}/movie_max.mp4"


def _build_movie_thumbnail(movie_id: Optional[int]) -> Optional[str]:
    if not movie_id:
        return None
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{movie_id}/movie.293x165.jpg"


def _parse_movies(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    movies = []
    fallback_thumb = (
        payload.get("header_image")
        or payload.get("capsule_image")
        or payload.get("background")
        or payload.get("background_raw")
    )
    for movie in payload.get("movies") or []:
        movie_id = movie.get("id")
        try:
            movie_id = int(movie_id) if movie_id is not None else None
        except (TypeError, ValueError):
            movie_id = None
        hls = movie.get("hls_h264") or movie.get("hls_av1")
        dash = movie.get("dash_h264") or movie.get("dash_av1")
        mp4 = movie.get("mp4") or {}
        webm = movie.get("webm") or {}
        url = mp4.get("max") or mp4.get("480") or webm.get("max") or webm.get("480")
        if not url:
            url = hls or dash
        if not url:
            url = _build_movie_fallback_url(movie_id)
        thumbnail = movie.get("thumbnail") or _build_movie_thumbnail(movie_id) or fallback_thumb
        if url:
            movies.append({"url": url, "thumbnail": thumbnail or "", "hls": hls, "dash": dash})
    return movies


def _parse_requirements(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    requirements = payload.get("pc_requirements") or {}
    minimum = _strip_html(requirements.get("minimum"))
    recommended = _strip_html(requirements.get("recommended"))
    if not minimum and not recommended:
        return None
    return {"minimum": minimum, "recommended": recommended}


def _summary_from_payload(appid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "app_id": str(appid),
        "name": payload.get("name") or str(appid),
        "short_description": _strip_html(payload.get("short_description")),
        "header_image": payload.get("header_image"),
        "capsule_image": payload.get("capsule_image") or payload.get("capsule_imagev5"),
        "background": payload.get("background"),
        "price": _parse_price(payload),
        "genres": _parse_genres(payload),
        "release_date": (payload.get("release_date") or {}).get("date"),
        "platforms": _parse_platforms(payload),
        "required_age": _parse_required_age(payload),
        "denuvo": str(appid) in DENUVO_APP_ID_SET,
    }


def _detail_from_payload(appid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    detail = _summary_from_payload(appid, payload)
    detail.update(
        {
            "about_the_game": _strip_html(payload.get("about_the_game")),
            "about_the_game_html": _sanitize_html(payload.get("about_the_game")),
            "detailed_description": _strip_html(payload.get("detailed_description")),
            "detailed_description_html": _sanitize_html(payload.get("detailed_description")),
            "developers": payload.get("developers") or [],
            "publishers": payload.get("publishers") or [],
            "categories": _parse_categories(payload),
            "screenshots": _parse_screenshots(payload),
            "movies": _parse_movies(payload),
            "pc_requirements": _parse_requirements(payload),
            "metacritic": payload.get("metacritic"),
            "recommendations": (payload.get("recommendations") or {}).get("total"),
            "website": payload.get("website"),
            "support_info": payload.get("support_info"),
        }
    )
    return detail


def _lua_dir() -> Path:
    # First check if lua sync service has cached files
    try:
        from .lua_sync import get_lua_files_dir
        synced_dir = get_lua_files_dir()
        if synced_dir.exists():
            return synced_dir
    except (ImportError, ValueError):
        pass
    
    if LUA_FILES_DIR:
        return Path(LUA_FILES_DIR)
    # PyInstaller bundled: look in _MEIPASS or next to exe
    import sys
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller bundle
        base_path = Path(getattr(sys, '_MEIPASS', sys.base_prefix))
        lua_path = base_path / "lua_files"
        if lua_path.exists():
            return lua_path
        # Also check next to exe
        exe_dir = Path(sys.executable).parent
        lua_path = exe_dir / "lua_files"
        if lua_path.exists():
            return lua_path
    return Path(__file__).resolve().parents[3] / "lua_files"


def _has_lua_files(lua_dir: Path) -> bool:
    try:
        from .native_lua_loader import verify_lua_dir
        count = verify_lua_dir(lua_dir)
        if count >= 0:
            return count > 0
    except (ValueError, OSError):
        pass
    try:
        return any(lua_dir.glob("*.lua"))
    except OSError:
        return False


def _attempt_lua_sync() -> None:
    try:
        if cache_client.get("lua:sync_attempt"):
            return
        from .lua_sync import sync_lua_files
        sync_lua_files()
        cache_client.set("lua:sync_attempt", "1", ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    except Exception:
        pass


def get_lua_appids() -> List[str]:
    cache_key = "steam:lua_appids"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached

    if LUA_REMOTE_ONLY:
        appids = get_lua_appids_from_server()
        cache_client.set_json(cache_key, appids, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
        return appids
    
    appids = []
    seen = set()
    lua_dir = _lua_dir()
    if not lua_dir.exists() or not _has_lua_files(lua_dir):
        _attempt_lua_sync()
        lua_dir = _lua_dir()
    if not lua_dir.exists() or not _has_lua_files(lua_dir):
        return []
    
    for item in lua_dir.glob("*.lua"):
        stem = item.stem.strip()
        appid = None
        if stem.isdigit():
            appid = stem
        else:
            match = re.search(r"\d{3,}", stem)
            if match:
                appid = match.group(0)
        if appid and appid not in seen:
            seen.add(appid)
            appids.append(appid)
        appids = sorted(appids, key=int)
    appids = prioritize_appids(appids)
    cache_client.set_json(cache_key, appids, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    return appids


def get_steam_summary(appid: str) -> Optional[dict]:
    cache_key = f"steam:summary:{appid}"
    cached = cache_client.get_json(cache_key)
    if cached:
        if "denuvo" not in cached:
            cached["denuvo"] = str(appid) in DENUVO_APP_ID_SET
        return cached
    data = _store_appdetails([appid], filters="basic,price_overview,platforms,genres,release_date")
    entry = data.get(str(appid), {})
    if not entry or not entry.get("success"):
        return None
    summary = _summary_from_payload(appid, entry.get("data") or {})
    cache_client.set_json(cache_key, summary, ttl=STEAM_CACHE_TTL_SECONDS)
    return summary


def get_steam_detail(appid: str) -> Optional[dict]:
    cache_key = f"steam:detail:{appid}"
    cached = cache_client.get_json(cache_key)
    if cached and cached.get("media_version") == MEDIA_VERSION:
        if "denuvo" not in cached:
            cached["denuvo"] = str(appid) in DENUVO_APP_ID_SET
        return cached
    data = _store_appdetails([appid])
    entry = data.get(str(appid), {})
    if not entry or not entry.get("success"):
        return cached
    detail = _detail_from_payload(appid, entry.get("data") or {})
    detail["media_version"] = MEDIA_VERSION
    cache_client.set_json(cache_key, detail, ttl=STEAM_CACHE_TTL_SECONDS)
    return detail


def get_catalog_page(appids: List[str]) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    missing: List[str] = []
    cached_map: Dict[str, Dict[str, Any]] = {}
    for appid in appids:
        cached = cache_client.get_json(f"steam:summary:{appid}")
        if cached:
            cached_map[appid] = cached
        else:
            missing.append(appid)

    fetched: Dict[str, Dict[str, Any]] = {}
    if missing:
        max_workers = min(8, len(missing))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(get_steam_summary, appid): appid for appid in missing}
            for future in as_completed(future_map):
                appid = future_map[future]
                try:
                    summary = future.result()
                except Exception:
                    summary = None
                if summary:
                    fetched[appid] = summary

    for appid in appids:
        summary = cached_map.get(appid) or fetched.get(appid)
        if summary:
            summaries.append(summary)

    return summaries


def search_store(term: str) -> List[Dict[str, Any]]:
    url = STEAM_STORE_SEARCH_URL or "https://store.steampowered.com/api/storesearch/"
    payload = _request(
        url,
        {
            "term": term,
            "l": "en",
            "cc": "us",
        },
    )
    items = payload.get("items") if payload else None
    if not items or not isinstance(items, list):
        return []
    results = []
    for item in items:
        price = item.get("price") or {}
        results.append(
            {
                "app_id": str(item.get("id")),
                "name": item.get("name"),
                "short_description": item.get("short_description"),
                "header_image": item.get("tiny_image"),
                "capsule_image": item.get("tiny_image"),
                "background": None,
                "required_age": None,
                "denuvo": str(item.get("id")) in DENUVO_APP_ID_SET,
                "price": {
                    "initial": price.get("initial"),
                    "final": price.get("final"),
                    "discount_percent": price.get("discount_percent"),
                    "currency": price.get("currency"),
                    "formatted": price.get("initial_formatted"),
                    "final_formatted": price.get("final_formatted"),
                }
                if price
                else None,
                "genres": None,
                "release_date": None,
                "platforms": None,
            }
        )
    return results
