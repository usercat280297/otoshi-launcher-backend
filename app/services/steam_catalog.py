from __future__ import annotations

import html
import json
import re
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Optional, Any, Dict, List
import ctypes
import os
import sys
import zipfile

import requests
import bleach

from ..core.cache import cache_client
from ..core.denuvo import DENUVO_APP_IDS, DENUVO_APP_ID_SET
from ..core.config import (
    LUA_FILES_DIR,
    LUA_REMOTE_ONLY,
    STEAM_CACHE_TTL_SECONDS,
    STEAM_CATALOG_CACHE_TTL_SECONDS,
    STEAM_APPDETAILS_BATCH_SIZE,
    STEAM_REQUEST_TIMEOUT_SECONDS,
    STEAM_STORE_API_URL,
    STEAM_STORE_SEARCH_URL,
    STEAM_TRENDING_CACHE_TTL_SECONDS,
    STEAM_TRENDING_LIMIT,
    STEAM_WEB_API_KEY,
    STEAM_WEB_API_URL,
)
from ..services.remote_game_data import get_lua_appids_from_server
from ..services.settings import normalize_locale as normalize_ui_locale

TAG_RE = re.compile(r"<[^>]+>")
MEDIA_VERSION = 7

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
_LUA_PACK_LOCK = threading.Lock()
_LUA_PACK_INDEX_SIGNATURE: Optional[str] = None
_LUA_PACK_INDEX: Optional[Dict[str, List[str]]] = None
_LUA_PACK_CLEANED_LEGACY = False
_CHUNK_MANIFEST_MAP_FILE = Path(__file__).resolve().parents[1] / "data" / "chunk_manifest_map.json"
_BYPASS_CATEGORIES_FILE = Path(__file__).resolve().parents[1] / "data" / "bypass_categories.json"
_ONLINE_FIX_FILE = Path(__file__).resolve().parents[1] / "data" / "online_fix.json"
_BYPASS_PRIORITY_CATEGORY_ORDER = ("others", "ea", "ubisoft", "rockstar")
_BYPASS_PRIORITY_CACHE_LOCK = threading.Lock()
_BYPASS_PRIORITY_CACHE_MTIME: Optional[float] = None
_BYPASS_PRIORITY_APPIDS: List[str] = []
_ONLINE_FIX_PRIORITY_CACHE_LOCK = threading.Lock()
_ONLINE_FIX_PRIORITY_CACHE_MTIME: Optional[float] = None
_ONLINE_FIX_PRIORITY_APPIDS: List[str] = []
_MANIFEST_NAME_MAP_LOCK = threading.Lock()
_MANIFEST_NAME_MAP_SIGNATURE: Optional[str] = None
_MANIFEST_NAME_MAP: Dict[str, str] = {}
_CONTENT_LOCALE_TO_STEAM_LANGUAGE: Dict[str, str] = {
    "en": "english",
    "vi": "vietnamese",
}


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


def _normalize_bypass_category_id(category_id: Any) -> str:
    normalized = str(category_id or "").strip().lower()
    if normalized in {"other", "others"}:
        return "others"
    return normalized


def _get_bypass_priority_appids() -> List[str]:
    global _BYPASS_PRIORITY_CACHE_MTIME
    global _BYPASS_PRIORITY_APPIDS

    try:
        mtime = _BYPASS_CATEGORIES_FILE.stat().st_mtime
    except OSError:
        return []

    with _BYPASS_PRIORITY_CACHE_LOCK:
        if _BYPASS_PRIORITY_CACHE_MTIME == mtime:
            return list(_BYPASS_PRIORITY_APPIDS)

        appids_by_category: Dict[str, List[str]] = {}
        try:
            payload = json.loads(_BYPASS_CATEGORIES_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            _BYPASS_PRIORITY_CACHE_MTIME = mtime
            _BYPASS_PRIORITY_APPIDS = []
            return []

        categories = payload.get("categories")
        if isinstance(categories, list):
            for category in categories:
                if not isinstance(category, dict):
                    continue
                category_id = _normalize_bypass_category_id(category.get("id"))
                raw_games = category.get("games")
                if not isinstance(raw_games, list):
                    continue
                bucket = appids_by_category.setdefault(category_id, [])
                for raw_app_id in raw_games:
                    app_id = str(raw_app_id or "").strip()
                    if app_id and app_id.isdigit():
                        bucket.append(app_id)

        ordered: List[str] = []
        seen: set[str] = set()
        for category_id in _BYPASS_PRIORITY_CATEGORY_ORDER:
            for app_id in appids_by_category.get(category_id, []):
                if app_id in seen:
                    continue
                seen.add(app_id)
                ordered.append(app_id)

        _BYPASS_PRIORITY_CACHE_MTIME = mtime
        _BYPASS_PRIORITY_APPIDS = ordered
        return list(ordered)


def _get_online_fix_priority_appids() -> List[str]:
    global _ONLINE_FIX_PRIORITY_CACHE_MTIME
    global _ONLINE_FIX_PRIORITY_APPIDS

    try:
        mtime = _ONLINE_FIX_FILE.stat().st_mtime
    except OSError:
        return []

    with _ONLINE_FIX_PRIORITY_CACHE_LOCK:
        if _ONLINE_FIX_PRIORITY_CACHE_MTIME == mtime:
            return list(_ONLINE_FIX_PRIORITY_APPIDS)

        try:
            payload = json.loads(_ONLINE_FIX_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            _ONLINE_FIX_PRIORITY_CACHE_MTIME = mtime
            _ONLINE_FIX_PRIORITY_APPIDS = []
            return []

        ordered = []
        if isinstance(payload, dict):
            ordered = sorted(
                (str(app_id).strip() for app_id in payload.keys() if str(app_id).strip().isdigit()),
                key=lambda value: int(value),
            )

        _ONLINE_FIX_PRIORITY_CACHE_MTIME = mtime
        _ONLINE_FIX_PRIORITY_APPIDS = ordered
        return list(ordered)


def _build_priority_appids() -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for source in (
        DENUVO_APP_IDS,
        _get_bypass_priority_appids(),
        _get_online_fix_priority_appids(),
        get_hot_appids(),
    ):
        for app_id in source:
            app_str = str(app_id).strip()
            if not app_str or app_str in seen:
                continue
            seen.add(app_str)
            ordered.append(app_str)
    return ordered


def prioritize_appids(appids: List[str]) -> List[str]:
    seen: set[str] = set()
    prioritized: list[str] = []
    appid_set = set(appids)
    for app_id in _build_priority_appids():
        if app_id in appid_set and app_id not in seen:
            prioritized.append(app_id)
            seen.add(app_id)
    for app_id in appids:
        if app_id not in seen:
            prioritized.append(app_id)
            seen.add(app_id)
    return prioritized


def prioritize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    priority_rank = {
        app_id: rank for rank, app_id in enumerate(_build_priority_appids())
    }
    priority_items: list[tuple[int, int, Dict[str, Any]]] = []
    other_items: list[tuple[int, Dict[str, Any]]] = []

    for index, item in enumerate(items):
        app_id = str(item.get("app_id") or "")
        rank = priority_rank.get(app_id)
        if rank is None:
            other_items.append((index, item))
        else:
            priority_items.append((rank, index, item))

    priority_items.sort(key=lambda row: (row[0], row[1]))
    ordered_priority = [item for _, _, item in priority_items]
    ordered_other = [item for _, item in other_items]
    return ordered_priority + ordered_other


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


def normalize_content_locale(locale: Optional[str]) -> str:
    return normalize_ui_locale(locale)


def _steam_language_for_locale(locale: Optional[str]) -> str:
    normalized = normalize_content_locale(locale)
    return _CONTENT_LOCALE_TO_STEAM_LANGUAGE.get(normalized, "english")


def _detail_cache_key(appid: str, locale: Optional[str]) -> str:
    normalized = normalize_content_locale(locale)
    if normalized == "en":
        return f"steam:detail:{appid}"
    return f"steam:detail:{appid}:locale:{normalized}"


def _store_appdetails(
    appids: Iterable[str],
    filters: Optional[str] = None,
    language: str = "english",
) -> Dict[str, Any]:
    url = f"{STEAM_STORE_API_URL.rstrip('/')}/appdetails"
    params = {
        "appids": ",".join(appids),
        "cc": "us",
        "l": language,
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


def _steam_fallback_images(appid: str) -> Dict[str, str]:
    base = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}"
    return {
        "header_image": f"{base}/header.jpg",
        "capsule_image": f"{base}/capsule_616x353.jpg",
        "background": f"{base}/library_hero.jpg",
    }


def _read_manifest_name_map() -> Dict[str, str]:
    global _MANIFEST_NAME_MAP_SIGNATURE, _MANIFEST_NAME_MAP

    if not _CHUNK_MANIFEST_MAP_FILE.exists():
        with _MANIFEST_NAME_MAP_LOCK:
            _MANIFEST_NAME_MAP_SIGNATURE = None
            _MANIFEST_NAME_MAP = {}
        return {}

    try:
        stat = _CHUNK_MANIFEST_MAP_FILE.stat()
        signature = f"{int(stat.st_mtime)}:{stat.st_size}"
    except OSError:
        return {}

    with _MANIFEST_NAME_MAP_LOCK:
        if _MANIFEST_NAME_MAP_SIGNATURE == signature:
            return dict(_MANIFEST_NAME_MAP)

        try:
            payload = json.loads(_CHUNK_MANIFEST_MAP_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            _MANIFEST_NAME_MAP_SIGNATURE = signature
            _MANIFEST_NAME_MAP = {}
            return {}

        steam_map = payload.get("steam_app_id") if isinstance(payload, dict) else None
        parsed: Dict[str, str] = {}
        if isinstance(steam_map, dict):
            for app_id, entry in steam_map.items():
                if not isinstance(entry, dict):
                    continue
                raw_name = (entry.get("game_name") or entry.get("folder") or "").strip()
                if raw_name:
                    parsed[str(app_id)] = raw_name

        _MANIFEST_NAME_MAP_SIGNATURE = signature
        _MANIFEST_NAME_MAP = parsed
        return dict(_MANIFEST_NAME_MAP)


def _fallback_summary_for_appid(appid: str) -> Dict[str, Any]:
    fallback = _steam_fallback_images(appid)
    mapped_name = _read_manifest_name_map().get(str(appid))
    return {
        "app_id": str(appid),
        "name": mapped_name or f"Steam App {appid}",
        "short_description": "Chunk manifest mapped title" if mapped_name else None,
        "header_image": fallback["header_image"],
        "capsule_image": fallback["capsule_image"],
        "background": fallback["background"],
        "price": None,
        "genres": [],
        "release_date": None,
        "platforms": ["windows"],
        "required_age": None,
        "dlc_count": 0,
        "item_type": None,
        "is_dlc": False,
        "denuvo": str(appid) in DENUVO_APP_ID_SET,
    }


def _summary_from_payload(appid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    dlc_raw = payload.get("dlc") or []
    dlc_count = len(dlc_raw) if isinstance(dlc_raw, list) else 0
    item_type = str(payload.get("type") or "").strip().lower() or None
    is_dlc = item_type == "dlc"
    fallback = _steam_fallback_images(appid)
    header_image = payload.get("header_image") or fallback["header_image"]
    capsule_image = payload.get("capsule_image") or payload.get("capsule_imagev5") or fallback["capsule_image"]
    background = payload.get("background") or payload.get("background_raw") or capsule_image or header_image or fallback["background"]
    return {
        "app_id": str(appid),
        "name": payload.get("name") or str(appid),
        "short_description": _strip_html(payload.get("short_description")),
        "header_image": header_image,
        "capsule_image": capsule_image,
        "background": background,
        "price": _parse_price(payload),
        "genres": _parse_genres(payload),
        "release_date": (payload.get("release_date") or {}).get("date"),
        "platforms": _parse_platforms(payload),
        "required_age": _parse_required_age(payload),
        "dlc_count": dlc_count,
        "item_type": item_type,
        "is_dlc": is_dlc,
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
    _cleanup_legacy_lua_extract()

    # First check if lua sync service has cached files
    try:
        from .lua_sync import get_lua_files_dir
        synced_dir = get_lua_files_dir()
        if synced_dir.exists() and _has_lua_files(synced_dir):
            return synced_dir
    except (ImportError, ValueError):
        pass
    
    if LUA_FILES_DIR:
        return Path(LUA_FILES_DIR)
    # PyInstaller bundled: look in _MEIPASS or next to exe
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


def _lua_cache_root() -> Path:
    cache_env = os.getenv("OTOSHI_CACHE_DIR", "").strip()
    if cache_env:
        return Path(cache_env)
    appdata = os.getenv("APPDATA", "").strip()
    if appdata:
        return Path(appdata) / "otoshi_launcher"
    return Path.cwd() / ".otoshi_cache"


def _resolve_lua_pack_path() -> Optional[Path]:
    env_path = os.getenv("LUA_PACK_PATH", "").strip()
    if env_path:
        candidate = Path(env_path)
        if candidate.exists():
            return candidate

    candidates: list[Path] = []
    exe_dir = Path(sys.executable).parent if getattr(sys, "frozen", False) else None

    if exe_dir:
        candidates.extend(
            [
                exe_dir / "lua.pack",
                exe_dir / "resources" / "backend" / "lua.pack",
                exe_dir / "backend" / "lua.pack",
            ]
        )
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        candidates.extend(
            [
                Path(meipass) / "lua.pack",
                Path(meipass) / "backend" / "lua.pack",
            ]
        )
    candidates.extend(
        [
            Path("lua.pack"),
            Path("resources") / "backend" / "lua.pack",
            Path("backend") / "lua.pack",
        ]
    )

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _extract_appid_from_name(name: str) -> Optional[str]:
    stem = Path(name).stem.strip()
    if stem.isdigit():
        return stem
    match = re.search(r"\d{3,}", stem)
    return match.group(0) if match else None


def _cleanup_legacy_lua_extract() -> None:
    global _LUA_PACK_CLEANED_LEGACY
    if _LUA_PACK_CLEANED_LEGACY:
        return
    _LUA_PACK_CLEANED_LEGACY = True

    cache_root = _lua_cache_root() / "lua_pack_cache"
    extract_root = cache_root / "extracted"
    marker_path = cache_root / ".source"

    try:
        if extract_root.exists():
            import shutil

            shutil.rmtree(extract_root, ignore_errors=True)
    except OSError:
        pass

    try:
        if marker_path.exists():
            marker_path.unlink()
    except OSError:
        pass


def _read_index_from_pack(archive: zipfile.ZipFile) -> List[str]:
    for candidate in ("appids.json", "lua_files/appids.json"):
        try:
            with archive.open(candidate, "r") as handle:
                import json

                raw = json.loads(handle.read().decode("utf-8", errors="ignore"))
        except KeyError:
            continue
        except Exception:
            return []
        if isinstance(raw, list):
            return [str(item) for item in raw if str(item).isdigit()]
    return []


def _read_lua_pack_index() -> Optional[Dict[str, List[str]]]:
    global _LUA_PACK_INDEX_SIGNATURE, _LUA_PACK_INDEX

    _cleanup_legacy_lua_extract()
    pack_path = _resolve_lua_pack_path()
    if not pack_path:
        return None

    try:
        signature = f"{pack_path.resolve()}|{pack_path.stat().st_size}|{int(pack_path.stat().st_mtime)}"
    except OSError:
        return None

    with _LUA_PACK_LOCK:
        if _LUA_PACK_INDEX_SIGNATURE == signature and _LUA_PACK_INDEX is not None:
            return _LUA_PACK_INDEX

        try:
            with zipfile.ZipFile(pack_path, "r") as archive:
                indexed = _read_index_from_pack(archive)
                appids: List[str] = []
                seen: set[str] = set()
                workshop_appids: List[str] = []
                workshop_seen: set[str] = set()

                for member in archive.infolist():
                    if member.is_dir():
                        continue
                    name = member.filename.replace("\\", "/")
                    if not name.lower().endswith(".lua"):
                        continue
                    appid = _extract_appid_from_name(Path(name).name)
                    if appid and appid not in seen:
                        seen.add(appid)
                        appids.append(appid)

                    if not appid or appid in workshop_seen:
                        continue

                    try:
                        with archive.open(member, "r") as handle:
                            for _ in range(20):
                                line = handle.readline()
                                if not line:
                                    break
                                if b"supports steam workshop content" in line.lower():
                                    workshop_seen.add(appid)
                                    workshop_appids.append(appid)
                                    break
                    except OSError:
                        continue

                if indexed:
                    ordered = []
                    used = set()
                    for appid in indexed:
                        if appid not in used:
                            used.add(appid)
                            ordered.append(appid)
                    for appid in appids:
                        if appid not in used:
                            used.add(appid)
                            ordered.append(appid)
                    appids = ordered
                elif appids:
                    appids = sorted(appids, key=int)

                if workshop_appids:
                    workshop_appids = sorted(workshop_appids, key=int)

                result = {
                    "appids": prioritize_appids(appids),
                    "workshop_appids": prioritize_appids(workshop_appids),
                }
                _LUA_PACK_INDEX_SIGNATURE = signature
                _LUA_PACK_INDEX = result
                return result
        except (OSError, zipfile.BadZipFile):
            _LUA_PACK_INDEX_SIGNATURE = signature
            _LUA_PACK_INDEX = {"appids": [], "workshop_appids": []}
            return _LUA_PACK_INDEX


def _has_lua_files(lua_dir: Path) -> bool:
    index_path = lua_dir / "appids.json"
    try:
        if index_path.exists() and index_path.stat().st_size > 2:
            return True
    except OSError:
        pass
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
        if cache_client.get("lua:sync_in_progress"):
            return

        cache_client.set("lua:sync_in_progress", "1", ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
        cache_client.set("lua:sync_attempt", "1", ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)

        def _run() -> None:
            try:
                from .lua_sync import sync_lua_files
                sync_lua_files()
            except Exception:
                pass
            finally:
                cache_client.delete("lua:sync_in_progress")

        threading.Thread(target=_run, daemon=True).start()
    except Exception:
        pass


def get_lua_appids() -> List[str]:
    cache_key = "steam:lua_appids"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        try:
            if len(cached) > 0:
                return cached
        except TypeError:
            return cached
        # Cached empty list: if lua files now exist, refresh instead of returning stale empty cache.
        lua_dir = _lua_dir()
        if lua_dir.exists() and _has_lua_files(lua_dir):
            cached = None
        else:
            pack_index = _read_lua_pack_index()
            if pack_index and pack_index.get("appids"):
                cached = None
            else:
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
        pack_index = _read_lua_pack_index()
        if pack_index and pack_index.get("appids"):
            appids = pack_index.get("appids", [])
            cache_client.set_json(cache_key, appids, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
            return appids
        return []

    # Fast path: use cached appid index if available
    index_path = lua_dir / "appids.json"
    if index_path.exists():
        try:
            import json
            raw = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(raw, list) and raw:
                appids = [str(x) for x in raw if str(x).isdigit()]
                appids = prioritize_appids(appids)
                cache_client.set_json(cache_key, appids, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
                return appids
        except Exception:
            pass
    
    for item in lua_dir.glob("*.lua"):
        appid = _extract_appid_from_name(item.name)
        if appid and appid not in seen:
            seen.add(appid)
            appids.append(appid)
    if appids:
        appids = sorted(appids, key=int)
    appids = prioritize_appids(appids)
    cache_client.set_json(cache_key, appids, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    return appids


def _lua_file_has_workshop_marker(path: Path) -> bool:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for _ in range(20):
                line = handle.readline()
                if not line:
                    break
                if "supports Steam Workshop content" in line:
                    return True
    except OSError:
        return False
    return False


def get_lua_workshop_appids() -> List[str]:
    cache_key = "steam:lua_workshop_appids"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        try:
            if len(cached) > 0:
                return cached
        except TypeError:
            return cached
        lua_dir = _lua_dir()
        if lua_dir.exists() and _has_lua_files(lua_dir):
            cached = None
        else:
            pack_index = _read_lua_pack_index()
            if pack_index and (pack_index.get("workshop_appids") or pack_index.get("appids")):
                cached = None
            else:
                return cached

    if LUA_REMOTE_ONLY:
        # Remote source does not expose workshop markers, fall back to all appids.
        appids = get_lua_appids()
        cache_client.set_json(cache_key, appids, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
        return appids

    appids: List[str] = []
    seen = set()
    lua_dir = _lua_dir()
    if not lua_dir.exists() or not _has_lua_files(lua_dir):
        _attempt_lua_sync()
        lua_dir = _lua_dir()
    if not lua_dir.exists() or not _has_lua_files(lua_dir):
        pack_index = _read_lua_pack_index()
        if pack_index:
            workshop = pack_index.get("workshop_appids") or []
            if workshop:
                cache_client.set_json(cache_key, workshop, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
                return workshop
        return get_lua_appids()

    for item in lua_dir.glob("*.lua"):
        if not _lua_file_has_workshop_marker(item):
            continue
        appid = _extract_appid_from_name(item.name)
        if appid and appid not in seen:
            seen.add(appid)
            appids.append(appid)

    if not appids:
        appids = get_lua_appids()
    else:
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


def get_steam_detail(appid: str, locale: Optional[str] = None) -> Optional[dict]:
    normalized_locale = normalize_content_locale(locale)
    cache_key = _detail_cache_key(appid, normalized_locale)
    cached = cache_client.get_json(cache_key)
    if cached and cached.get("media_version") == MEDIA_VERSION:
        if "denuvo" not in cached:
            cached["denuvo"] = str(appid) in DENUVO_APP_ID_SET
        if "content_locale" not in cached:
            cached["content_locale"] = normalized_locale
        return cached

    steam_language = _steam_language_for_locale(normalized_locale)
    data = _store_appdetails([appid], language=steam_language)
    entry = data.get(str(appid), {})
    if not entry or not entry.get("success"):
        if normalized_locale != "en":
            fallback = get_steam_detail(appid, locale="en")
            if fallback:
                return fallback
        return cached
    detail = _detail_from_payload(appid, entry.get("data") or {})
    detail["media_version"] = MEDIA_VERSION
    detail["content_locale"] = normalized_locale
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
        batch_size = max(1, STEAM_APPDETAILS_BATCH_SIZE)
        # Batch appdetails requests to avoid N parallel HTTP calls (faster and more reliable).
        for i in range(0, len(missing), batch_size):
            batch = missing[i : i + batch_size]
            payload = _store_appdetails(batch, filters="basic,price_overview,platforms,genres,release_date")
            if not payload and len(batch) > 1:
                # Steam store API may reject multi-appid requests (returns 400/null).
                # Fall back to per-appid requests to keep catalog usable.
                for appid in batch:
                    single = _store_appdetails([appid], filters="basic,price_overview,platforms,genres,release_date")
                    if single:
                        payload.update(single)
            for appid in batch:
                entry = payload.get(str(appid), {}) if isinstance(payload, dict) else {}
                if not entry or not entry.get("success"):
                    continue
                summary = _summary_from_payload(appid, entry.get("data") or {})
                cache_client.set_json(f"steam:summary:{appid}", summary, ttl=STEAM_CACHE_TTL_SECONDS)
                fetched[appid] = summary

    for appid in appids:
        summary = cached_map.get(appid) or fetched.get(appid)
        if summary:
            summaries.append(summary)
            continue

        fallback = _fallback_summary_for_appid(appid)
        cache_client.set_json(f"steam:summary:{appid}", fallback, ttl=STEAM_CACHE_TTL_SECONDS)
        summaries.append(fallback)

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
        item_type = str(item.get("type") or "").strip().lower() or None
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
                "item_type": item_type,
                "is_dlc": item_type == "dlc",
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
