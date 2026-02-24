from __future__ import annotations

import re
import json
import time
import subprocess
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple
from pathlib import Path

import requests
from sqlalchemy import and_, case, desc, func, or_, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from threading import Lock

from ..core.config import (
    CROSS_STORE_MAPPING_ENABLED,
    CROSS_STORE_MAPPING_MIN_CONFIDENCE,
    EPIC_CATALOG_COUNTRY,
    EPIC_CATALOG_FREE_GAMES_URL,
    EPIC_CATALOG_LOCALE,
    STEAMDB_BASE_URL,
    STEAMDB_ENRICHMENT_ENABLED,
    STEAMDB_ENRICHMENT_MAX_ITEMS,
    STEAMDB_REQUEST_TIMEOUT_SECONDS,
    STEAM_GLOBAL_INDEX_EPIC_CONFIDENCE_THRESHOLD,
    STEAM_GLOBAL_INDEX_ENFORCE_COMPLETE,
    STEAM_GLOBAL_INDEX_COMPLETION_BATCH,
    STEAM_GLOBAL_INDEX_INGEST_BATCH,
    STEAM_GLOBAL_INDEX_MAX_PREFETCH,
    STEAM_GLOBAL_INDEX_SEARCH_LIMIT,
    STEAM_GO_CRAWLER_BIN,
    STEAM_GO_CRAWLER_ENABLED,
    STEAM_GO_CRAWLER_TIMEOUT_SECONDS,
    STEAM_REQUEST_TIMEOUT_SECONDS,
    STEAM_WEB_API_KEY,
    STEAM_WEB_API_URL,
)
from ..db import Base, engine
from ..models import (
    AssetJob,
    CrossStoreMapping,
    IngestCursor,
    IngestJob,
    SteamDbEnrichment,
    SteamTitle,
    SteamTitleAlias,
    SteamTitleAsset,
    SteamTitleMetadata,
)
from ..core.denuvo import DENUVO_APP_ID_SET, DENUVO_APP_IDS
from .steam_catalog import get_hot_appids, get_lua_appids, get_steam_detail, get_steam_summary
from .steamgriddb import build_steam_fallback_assets, resolve_assets

_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
_DLC_HINTS = re.compile(
    r"\b(dlc|soundtrack|season pass|expansion|pack|set|skin|costume|mission|bonus|artbook|pachislot)\b",
    re.IGNORECASE,
)
_PLACEHOLDER_STEAM_APP_PATTERN = re.compile(r"^steam app\s+\d+$", re.IGNORECASE)
_SCHEMA_LOCK = Lock()
_SCHEMA_READY = False
_CHUNK_MANIFEST_MAP_FILE = Path(__file__).resolve().parents[1] / "data" / "chunk_manifest_map.json"
_STEAMDB_JSON_PATTERN = re.compile(
    r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
_YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
_EPIC_CACHE_LOCK = Lock()
_EPIC_CANDIDATE_CACHE: Dict[str, Any] = {"loaded_at": 0.0, "items": []}
_BYPASS_CATEGORIES_FILE = Path(__file__).resolve().parents[1] / "data" / "bypass_categories.json"
_ONLINE_FIX_FILE = Path(__file__).resolve().parents[1] / "data" / "online_fix.json"
_BYPASS_PRIORITY_CATEGORY_ORDER = ("others", "ea", "ubisoft", "rockstar")
_BYPASS_PRIORITY_LOCK = Lock()
_BYPASS_PRIORITY_CACHE_MTIME: Optional[float] = None
_BYPASS_PRIORITY_APPIDS: List[str] = []
_ONLINE_FIX_PRIORITY_LOCK = Lock()
_ONLINE_FIX_PRIORITY_CACHE_MTIME: Optional[float] = None
_ONLINE_FIX_PRIORITY_APPIDS: List[str] = []
_LUA_FALLBACK_SEED_RETRY_ATTEMPTS = 15
_LUA_FALLBACK_SEED_RETRY_DELAY_SECONDS = 2


def _looks_like_generic_epic_badge(url: str) -> bool:
    text = (url or "").strip().lower()
    if not text:
        return True
    noisy_tokens = (
        "epic-games-store-logo",
        "epic-store-logo",
        "egs-logo",
        "epic_badge",
        "epic-badge",
        "/badges/epic",
        "logo-epic",
    )
    return any(token in text for token in noisy_tokens)


def _normalize_bypass_category_id(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"other", "others"}:
        return "others"
    return normalized


def _load_bypass_priority_appids() -> List[str]:
    global _BYPASS_PRIORITY_CACHE_MTIME
    global _BYPASS_PRIORITY_APPIDS

    try:
        mtime = _BYPASS_CATEGORIES_FILE.stat().st_mtime
    except OSError:
        return []

    with _BYPASS_PRIORITY_LOCK:
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


def _load_online_fix_priority_appids() -> List[str]:
    global _ONLINE_FIX_PRIORITY_CACHE_MTIME
    global _ONLINE_FIX_PRIORITY_APPIDS

    try:
        mtime = _ONLINE_FIX_FILE.stat().st_mtime
    except OSError:
        return []

    with _ONLINE_FIX_PRIORITY_LOCK:
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


def _build_priority_candidate_appids(scope_set: Optional[set[str]] = None) -> List[str]:
    candidates: List[str] = []
    for source in (
        DENUVO_APP_IDS,
        _load_bypass_priority_appids(),
        _load_online_fix_priority_appids(),
        get_hot_appids(),
    ):
        for app_id in source:
            app_str = str(app_id).strip()
            if not app_str:
                continue
            if scope_set is not None and app_str not in scope_set:
                continue
            candidates.append(app_str)
    return candidates


def ensure_global_index_schema(force: bool = False) -> None:
    """
    Ensure new global-index tables exist even on upgraded installs that still
    have an older sqlite file.
    """
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY and not force:
            return
        try:
            Base.metadata.create_all(bind=engine)
        except OperationalError as exc:
            # SQLite schema init may race in portable/multi-process startup.
            if "already exists" not in str(exc).lower():
                raise
        _SCHEMA_READY = True


def _with_schema_retry(action):
    ensure_global_index_schema()
    try:
        return action()
    except OperationalError as exc:
        # Handle "no such table" gracefully on stale DBs.
        if "no such table" not in str(exc).lower():
            raise
        ensure_global_index_schema(force=True)
        return action()


def normalize_title(value: str) -> str:
    cleaned = _NON_ALNUM.sub(" ", (value or "").strip().lower())
    return " ".join(cleaned.split())


def _compact_alnum(value: str) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _split_compact_query(value: str) -> Tuple[str, str]:
    compact = _compact_alnum(value)
    letters = "".join(ch for ch in compact if ch.isalpha())
    digits = "".join(ch for ch in compact if ch.isdigit())
    return letters, digits


def _build_initials_like_pattern(letters: str) -> str:
    normalized = "".join(ch for ch in str(letters or "").lower() if ch.isalpha())
    if len(normalized) < 2:
        return ""
    return " ".join(f"{ch}%" for ch in normalized)


def _is_placeholder_title_name(name: Any, app_id: Optional[str] = None) -> bool:
    text = str(name or "").strip()
    if not text:
        return True

    lowered = text.lower()
    if _PLACEHOLDER_STEAM_APP_PATTERN.match(text):
        return True
    if text.isdigit():
        return True
    if app_id and lowered in {str(app_id).strip().lower(), f"steam app {str(app_id).strip().lower()}"}:
        return True
    return False


def _pick_best_title_name(app_id: str, *candidates: Any) -> str:
    app_id_text = str(app_id or "").strip()
    normalized: List[str] = []
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            normalized.append(text)

    for text in normalized:
        if not _is_placeholder_title_name(text, app_id_text):
            return text

    if normalized:
        return normalized[0]

    return f"Steam App {app_id_text}" if app_id_text else ""


def _steam_applist_url() -> str:
    return f"{STEAM_WEB_API_URL.rstrip('/')}/ISteamApps/GetAppList/v2/"


def _steam_store_applist_url() -> str:
    return f"{STEAM_WEB_API_URL.rstrip('/')}/IStoreService/GetAppList/v1/"


def _steamdb_base_url() -> str:
    return STEAMDB_BASE_URL.rstrip("/")


def _steamdb_app_data_url(app_id: str) -> str:
    return f"{_steamdb_base_url()}/api/GetAppData/?appid={app_id}"


def _steamdb_price_url(app_id: str) -> str:
    return f"{_steamdb_base_url()}/api/GetPrice/?appid={app_id}&cc=us"


def _steamdb_html_url(app_id: str) -> str:
    return f"{_steamdb_base_url()}/app/{app_id}/"


def _request_json_with_status(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    try:
        response = requests.get(
            url,
            params=params,
            timeout=timeout or STEAMDB_REQUEST_TIMEOUT_SECONDS,
            headers={
                "User-Agent": "otoshi-launcher/1.0 (+https://otoshi.app)",
                "Accept": "application/json,text/plain,*/*",
            },
        )
    except requests.RequestException:
        return None, None

    status = int(response.status_code)
    if status != 200:
        return None, status
    try:
        payload = response.json()
    except ValueError:
        return None, status
    if not isinstance(payload, dict):
        return None, status
    return payload, status


def _request_text_with_status(
    url: str,
    *,
    timeout: Optional[int] = None,
) -> Tuple[Optional[str], Optional[int]]:
    try:
        response = requests.get(
            url,
            timeout=timeout or STEAMDB_REQUEST_TIMEOUT_SECONDS,
            headers={
                "User-Agent": "otoshi-launcher/1.0 (+https://otoshi.app)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
        )
    except requests.RequestException:
        return None, None
    status = int(response.status_code)
    if status != 200:
        return None, status
    return response.text, status


def _extract_release_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        if 1900 <= value <= 2100:
            return value
        return None
    match = _YEAR_PATTERN.search(str(value))
    if not match:
        return None
    try:
        year = int(match.group(0))
    except ValueError:
        return None
    if 1900 <= year <= 2100:
        return year
    return None


def _tokenize_title(value: str) -> set[str]:
    normalized = normalize_title(value)
    if not normalized:
        return set()
    return {piece for piece in normalized.split(" ") if piece}


def _title_similarity_score(left: str, right: str) -> float:
    a = normalize_title(left)
    b = normalize_title(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    ratio = SequenceMatcher(a=a, b=b).ratio()
    tokens_a = _tokenize_title(a)
    tokens_b = _tokenize_title(b)
    if tokens_a and tokens_b:
        intersection = len(tokens_a.intersection(tokens_b))
        union = len(tokens_a.union(tokens_b))
        jaccard = (intersection / union) if union else 0.0
    else:
        jaccard = 0.0
    prefix_bonus = 0.05 if (a.startswith(b) or b.startswith(a)) else 0.0
    return max(0.0, min(1.0, (ratio * 0.62) + (jaccard * 0.33) + prefix_bonus))


def _extract_ld_json_blocks(html_payload: str) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for raw in _STEAMDB_JSON_PATTERN.findall(html_payload or ""):
        candidate = raw.strip()
        if not candidate:
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            blocks.append(parsed)
        elif isinstance(parsed, list):
            for entry in parsed:
                if isinstance(entry, dict):
                    blocks.append(entry)
    return blocks


def _coerce_json_list(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        normalized: List[Any] = []
        for key, value in raw.items():
            if isinstance(value, dict):
                normalized.append({"id": key, **value})
            else:
                normalized.append({"id": key, "value": value})
        return normalized
    return []


def _extract_steamdb_price_history(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not payload:
        return []
    candidates: List[Any] = []
    for key in ("prices", "price_history", "data", "result"):
        value = payload.get(key)
        if value:
            candidates.append(value)
    if not candidates:
        candidates.append(payload)

    history: List[Dict[str, Any]] = []
    for candidate in candidates:
        if isinstance(candidate, list):
            for entry in candidate:
                if not isinstance(entry, dict):
                    continue
                row = {
                    "currency": entry.get("currency"),
                    "current": entry.get("current") or entry.get("final"),
                    "lowest": entry.get("lowest"),
                    "discount_percent": entry.get("discount_percent") or entry.get("discount"),
                    "date": entry.get("date") or entry.get("updated_at"),
                }
                if any(value is not None for value in row.values()):
                    history.append(row)
        elif isinstance(candidate, dict):
            for currency, entry in candidate.items():
                if not isinstance(entry, dict):
                    continue
                row = {
                    "currency": currency,
                    "current": entry.get("current") or entry.get("final"),
                    "lowest": entry.get("lowest"),
                    "discount_percent": entry.get("discount_percent") or entry.get("discount"),
                    "date": entry.get("date") or entry.get("updated_at"),
                }
                if any(value is not None for value in row.values()):
                    history.append(row)
    return history[:200]


def _extract_steamdb_enrichment(
    app_id: str,
) -> Dict[str, Any]:
    status_codes: Dict[str, Optional[int]] = {}
    app_data, app_status = _request_json_with_status(_steamdb_app_data_url(app_id))
    status_codes["steamdb_api_app_data"] = app_status
    price_data, price_status = _request_json_with_status(_steamdb_price_url(app_id))
    status_codes["steamdb_api_price"] = price_status
    html_data, html_status = _request_text_with_status(_steamdb_html_url(app_id))
    status_codes["steamdb_html"] = html_status

    ld_blocks = _extract_ld_json_blocks(html_data or "")
    app_payload = app_data or {}
    app_payload_data = app_payload.get("data") if isinstance(app_payload.get("data"), dict) else app_payload

    hidden_tags_raw = (
        app_payload_data.get("hidden_tags")
        or app_payload_data.get("hiddenTags")
        or app_payload_data.get("tags")
        or []
    )
    hidden_tags = [str(item).strip() for item in _coerce_json_list(hidden_tags_raw) if str(item).strip()]
    if not hidden_tags and ld_blocks:
        for block in ld_blocks:
            genre_value = block.get("genre")
            if isinstance(genre_value, list):
                hidden_tags = [str(item).strip() for item in genre_value if str(item).strip()]
                if hidden_tags:
                    break
            elif isinstance(genre_value, str) and genre_value.strip():
                hidden_tags = [genre_value.strip()]
                break

    depots = _coerce_json_list(app_payload_data.get("depots") or app_payload_data.get("Depot") or [])
    branches_raw = app_payload_data.get("branches") or app_payload_data.get("branch_map") or {}
    branch_map = branches_raw if isinstance(branches_raw, dict) else {}
    price_history = _extract_steamdb_price_history(price_data)

    confidence = 0.0
    if app_data:
        confidence += 0.5
    if price_history:
        confidence += 0.25
    if ld_blocks:
        confidence += 0.2
    if depots:
        confidence += 0.1
    if branch_map:
        confidence += 0.05
    confidence = max(0.0, min(confidence, 1.0))

    source = "steamdb_structured" if confidence >= 0.35 else "steamdb_unavailable"
    payload: Dict[str, Any] = {
        "app_id": str(app_id),
        "status_codes": status_codes,
        "blocked": any(code == 403 for code in status_codes.values() if code is not None),
        "fetched_at": datetime.utcnow().isoformat(),
    }
    if app_data:
        payload["app_data"] = app_data
    if price_data:
        payload["price_data"] = price_data
    if ld_blocks:
        payload["ld_json"] = ld_blocks

    return {
        "price_history": price_history,
        "hidden_tags": hidden_tags[:200],
        "depots": depots[:500],
        "branch_map": branch_map,
        "payload": payload,
        "confidence": confidence,
        "source": source,
    }


def _upsert_steamdb_row(
    db: Session,
    title: SteamTitle,
    enrichment: Dict[str, Any],
) -> float:
    row = (
        db.query(SteamDbEnrichment)
        .filter(SteamDbEnrichment.steam_title_id == title.id)
        .first()
    )
    if row is None:
        row = SteamDbEnrichment(steam_title_id=title.id)
        db.add(row)

    row.price_history = enrichment.get("price_history") or []
    row.hidden_tags = enrichment.get("hidden_tags") or []
    row.depots = enrichment.get("depots") or []
    row.branch_map = enrichment.get("branch_map") or {}
    row.payload = enrichment.get("payload") or {}
    row.confidence = float(enrichment.get("confidence") or 0.0)
    row.source = str(enrichment.get("source") or "steamdb_structured")
    return float(row.confidence or 0.0)


def _extract_epic_candidate_assets(item: Dict[str, Any]) -> Dict[str, Optional[str]]:
    images = item.get("keyImages") if isinstance(item.get("keyImages"), list) else []
    selected: Dict[str, Optional[str]] = {"grid": None, "hero": None, "logo": None, "icon": None}

    def _pick(types: set[str], *, allow_generic: bool = False) -> Optional[str]:
        best_url: Optional[str] = None
        best_score = -1
        for entry in images:
            if not isinstance(entry, dict):
                continue
            image_type = str(entry.get("type") or "").strip().lower()
            url = str(entry.get("url") or "").strip()
            if not url or image_type not in types:
                continue
            if not allow_generic and _looks_like_generic_epic_badge(url):
                continue
            width = int(entry.get("width") or 0)
            height = int(entry.get("height") or 0)
            score = width * height
            if "1200x1600" in url.lower() or "600x900" in url.lower():
                score += 500_000
            if "2560x1440" in url.lower() or "1920x1080" in url.lower():
                score += 250_000
            if score > best_score:
                best_score = score
                best_url = url
        return best_url

    selected["grid"] = _pick(
        {
            "offerimagetall",
            "dieselstorefronttall",
            "offerimageportrait",
            "thumbnail",
        }
    )
    selected["hero"] = _pick(
        {
            "offerimagewide",
            "dieselstorefrontwide",
            "featuredmedia",
            "hero",
            "background",
        }
    )
    selected["logo"] = _pick({"logo", "offerlogo", "diesellogo"}, allow_generic=True)
    selected["icon"] = _pick({"icon", "square", "thumbnail", "dieselstorefrontsmall"})

    if selected["grid"] is None:
        selected["grid"] = selected["hero"] or selected["icon"]
    if selected["hero"] is None:
        selected["hero"] = selected["grid"] or selected["logo"] or selected["icon"]
    if selected["icon"] is None:
        selected["icon"] = selected["grid"] or selected["logo"] or selected["hero"]
    return selected


def _build_epic_candidates(force_refresh: bool = False) -> List[Dict[str, Any]]:
    if not CROSS_STORE_MAPPING_ENABLED:
        return []

    now = time.time()
    with _EPIC_CACHE_LOCK:
        cached_items = _EPIC_CANDIDATE_CACHE.get("items") or []
        loaded_at = float(_EPIC_CANDIDATE_CACHE.get("loaded_at") or 0.0)
        if not force_refresh and cached_items and (now - loaded_at) < 1800:
            return list(cached_items)

    payload, status = _request_json_with_status(
        EPIC_CATALOG_FREE_GAMES_URL,
        params={
            "locale": EPIC_CATALOG_LOCALE,
            "country": EPIC_CATALOG_COUNTRY,
            "allowCountries": EPIC_CATALOG_COUNTRY,
        },
        timeout=STEAM_REQUEST_TIMEOUT_SECONDS,
    )
    if status != 200 or not payload:
        with _EPIC_CACHE_LOCK:
            return list(_EPIC_CANDIDATE_CACHE.get("items") or [])

    elements = (
        (payload.get("data") or {})
        .get("Catalog", {})
        .get("searchStore", {})
        .get("elements", [])
    )
    if not isinstance(elements, list):
        elements = []

    deduped: Dict[str, Dict[str, Any]] = {}
    for item in elements:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        epic_product_id = str(item.get("id") or item.get("productSlug") or "").strip()
        if not epic_product_id:
            continue
        mappings = (
            (item.get("catalogNs") or {}).get("mappings")
            if isinstance(item.get("catalogNs"), dict)
            else []
        )
        page_slug = None
        if isinstance(mappings, list):
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    continue
                slug = str(mapping.get("pageSlug") or "").strip()
                if slug:
                    page_slug = slug
                    break
        deduped[epic_product_id] = {
            "epic_product_id": epic_product_id,
            "title": title,
            "normalized_title": normalize_title(title),
            "release_year": _extract_release_year(item.get("releaseDate")),
            "developer": str(item.get("developerDisplayName") or "").strip() or None,
            "seller": (
                str((item.get("seller") or {}).get("name") or "").strip()
                if isinstance(item.get("seller"), dict)
                else None
            ),
            "page_slug": page_slug,
            "assets": _extract_epic_candidate_assets(item),
        }

    candidates = list(deduped.values())
    with _EPIC_CACHE_LOCK:
        if candidates:
            _EPIC_CANDIDATE_CACHE["items"] = list(candidates)
            _EPIC_CANDIDATE_CACHE["loaded_at"] = now
        return list(_EPIC_CANDIDATE_CACHE.get("items") or candidates)


def _select_cross_store_match(
    title: SteamTitle,
    epic_candidates: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not epic_candidates:
        return None

    steam_title = title.name or ""
    if not steam_title:
        return None

    steam_year = _extract_release_year(title.release_date)
    steam_developer_tokens = _tokenize_title(title.developer or "")

    best_candidate: Optional[Dict[str, Any]] = None
    best_score = -1.0
    for candidate in epic_candidates:
        candidate_title = str(candidate.get("title") or "")
        score = _title_similarity_score(steam_title, candidate_title)

        candidate_year = _extract_release_year(candidate.get("release_year"))
        if steam_year and candidate_year:
            delta = abs(steam_year - candidate_year)
            if delta == 0:
                score += 0.06
            elif delta == 1:
                score += 0.03
            elif delta >= 5:
                score -= 0.08

        if steam_developer_tokens:
            candidate_dev_tokens = _tokenize_title(candidate.get("developer") or "")
            candidate_seller_tokens = _tokenize_title(candidate.get("seller") or "")
            if steam_developer_tokens.intersection(candidate_dev_tokens):
                score += 0.04
            elif steam_developer_tokens.intersection(candidate_seller_tokens):
                score += 0.02

        score = max(0.0, min(1.0, score))
        if score > best_score:
            best_candidate = candidate
            best_score = score

    if best_candidate is None:
        return None
    if best_score < CROSS_STORE_MAPPING_MIN_CONFIDENCE:
        return None

    return {"candidate": best_candidate, "confidence": best_score}


def _upsert_cross_store_mapping(
    db: Session,
    title: SteamTitle,
    matched: Dict[str, Any],
) -> float:
    candidate = matched.get("candidate") or {}
    confidence = float(matched.get("confidence") or 0.0)
    epic_product_id = str(candidate.get("epic_product_id") or "").strip()
    if not epic_product_id:
        return 0.0

    existing = (
        db.query(CrossStoreMapping)
        .filter(CrossStoreMapping.steam_app_id == title.app_id)
        .order_by(CrossStoreMapping.confidence.desc(), CrossStoreMapping.updated_at.desc())
        .first()
    )
    if existing is None:
        existing = CrossStoreMapping(
            steam_app_id=title.app_id,
            epic_product_id=epic_product_id,
        )
        db.add(existing)

    # Replace mapping when score improves or when mapping matches same Epic product.
    should_update = (
        existing.epic_product_id == epic_product_id
        or float(existing.confidence or 0.0) <= confidence
    )
    if not should_update:
        return float(existing.confidence or 0.0)

    existing.epic_product_id = epic_product_id
    existing.confidence = confidence
    existing.evidence = {
        "matched_title": candidate.get("title"),
        "steam_title": title.name,
        "page_slug": candidate.get("page_slug"),
        "developer": candidate.get("developer"),
        "seller": candidate.get("seller"),
        "assets": candidate.get("assets") or {},
        "score": confidence,
        "verification_state": "matched",
        "evidence_source": "epic_catalog",
        "updated_at": datetime.utcnow().isoformat(),
    }
    return confidence


def _is_empty_text(value: Any) -> bool:
    return not isinstance(value, str) or not value.strip()


def _ensure_metadata_complete(
    db: Session,
    title: SteamTitle,
    steam_assets: Dict[str, Optional[str]],
) -> Tuple[bool, bool]:
    metadata = title.metadata_row
    created = False
    changed = False
    if metadata is None:
        metadata = SteamTitleMetadata(steam_title_id=title.id)
        db.add(metadata)
        title.metadata_row = metadata
        created = True
        changed = True

    summary_payload = metadata.summary_payload if isinstance(metadata.summary_payload, dict) else {}
    detail_payload = metadata.detail_payload if isinstance(metadata.detail_payload, dict) else {}

    default_short = f"{title.name} is available in Otoshi global catalog."
    default_item_type = str(title.title_type or "").strip().lower() or "game"
    default_platforms = metadata.platforms if isinstance(metadata.platforms, list) and metadata.platforms else []
    if not default_platforms:
        flag_map = title.platform_flags if isinstance(title.platform_flags, dict) else {}
        default_platforms = [key for key, enabled in flag_map.items() if enabled]
        if not default_platforms:
            default_platforms = ["windows"]

    summary_defaults = {
        "app_id": title.app_id,
        "name": title.name,
        "short_description": metadata.short_description or default_short,
        "header_image": summary_payload.get("header_image") or steam_assets.get("grid"),
        "capsule_image": summary_payload.get("capsule_image") or steam_assets.get("grid"),
        "background": summary_payload.get("background") or steam_assets.get("hero"),
        "platforms": summary_payload.get("platforms") or default_platforms,
        "item_type": summary_payload.get("item_type") or default_item_type,
    }
    for key, value in summary_defaults.items():
        current = summary_payload.get(key)
        if current is None or (isinstance(current, str) and not current.strip()) or (isinstance(current, list) and not current):
            summary_payload[key] = value
            changed = True

    detail_defaults = {
        "app_id": title.app_id,
        "name": title.name,
        "short_description": detail_payload.get("short_description") or summary_payload.get("short_description"),
        "about_the_game": detail_payload.get("about_the_game") or summary_payload.get("short_description"),
        "about_the_game_html": detail_payload.get("about_the_game_html"),
        "detailed_description": detail_payload.get("detailed_description") or summary_payload.get("short_description"),
        "detailed_description_html": detail_payload.get("detailed_description_html"),
        "header_image": detail_payload.get("header_image") or summary_payload.get("header_image"),
        "capsule_image": detail_payload.get("capsule_image") or summary_payload.get("capsule_image"),
        "background": detail_payload.get("background") or summary_payload.get("background"),
        "developers": detail_payload.get("developers") or ([title.developer] if title.developer else []),
        "publishers": detail_payload.get("publishers") or ([title.publisher] if title.publisher else []),
        "genres": detail_payload.get("genres") or metadata.genres or [],
        "platforms": detail_payload.get("platforms") or summary_payload.get("platforms"),
        "item_type": detail_payload.get("item_type") or summary_payload.get("item_type"),
        "is_dlc": bool(detail_payload.get("is_dlc")) if "is_dlc" in detail_payload else bool(default_item_type == "dlc"),
        "screenshots": detail_payload.get("screenshots") or [
            value for value in [summary_payload.get("header_image"), summary_payload.get("background")] if value
        ],
        "movies": detail_payload.get("movies") or [],
        "pc_requirements": detail_payload.get("pc_requirements") or {},
        "metacritic": detail_payload.get("metacritic"),
        "recommendations": detail_payload.get("recommendations"),
        "website": detail_payload.get("website"),
        "support_info": detail_payload.get("support_info"),
    }
    for key, value in detail_defaults.items():
        current = detail_payload.get(key)
        if current is None or (isinstance(current, str) and not current.strip()) or (isinstance(current, list) and not current):
            detail_payload[key] = value
            changed = True

    if _is_empty_text(metadata.short_description):
        metadata.short_description = str(summary_payload.get("short_description") or default_short)
        changed = True
    if _is_empty_text(metadata.long_description):
        metadata.long_description = str(detail_payload.get("detailed_description") or metadata.short_description or default_short)
        changed = True
    if not isinstance(metadata.genres, list):
        metadata.genres = []
        changed = True
    if not isinstance(metadata.platforms, list) or not metadata.platforms:
        metadata.platforms = list(summary_payload.get("platforms") or default_platforms)
        changed = True
    if not isinstance(metadata.requirements, dict):
        metadata.requirements = {}
        changed = True
    if not isinstance(metadata.reviews, dict):
        metadata.reviews = {}
        changed = True
    if not isinstance(metadata.players, dict):
        metadata.players = {}
        changed = True
    if not isinstance(metadata.dlc_graph, dict):
        metadata.dlc_graph = {}
        changed = True

    metadata.summary_payload = summary_payload
    metadata.detail_payload = detail_payload
    metadata.last_refreshed_at = datetime.utcnow()
    return created, changed


def _ensure_assets_complete(
    db: Session,
    title: SteamTitle,
    steam_assets: Dict[str, Optional[str]],
) -> Tuple[bool, bool]:
    asset_row = title.assets_row
    created = False
    changed = False
    if asset_row is None:
        asset_row = SteamTitleAsset(steam_title_id=title.id)
        db.add(asset_row)
        title.assets_row = asset_row
        created = True
        changed = True

    selected_source = str(asset_row.selected_source or "").strip().lower() or "steam"
    selected_assets = asset_row.selected_assets if isinstance(asset_row.selected_assets, dict) else {}
    if not selected_assets:
        selected_assets = dict(steam_assets)
        selected_source = "steam"
        changed = True

    # Never keep generic Epic badge as main art.
    if selected_source == "epic":
        noisy = _looks_like_generic_epic_badge(selected_assets.get("icon") or "") and _looks_like_generic_epic_badge(
            selected_assets.get("logo") or ""
        )
        if noisy:
            selected_assets = dict(steam_assets)
            selected_source = "steam"
            changed = True

    normalized = _normalize_selected_assets(title.app_id, selected_assets, steam_assets)
    if normalized != selected_assets:
        selected_assets = normalized
        changed = True

    if not isinstance(asset_row.sgdb_assets, dict):
        asset_row.sgdb_assets = {}
        changed = True
    if not isinstance(asset_row.epic_assets, dict):
        asset_row.epic_assets = {}
        changed = True
    if not isinstance(asset_row.steam_assets, dict) or not asset_row.steam_assets:
        asset_row.steam_assets = dict(steam_assets)
        changed = True

    asset_row.selected_assets = selected_assets
    asset_row.selected_source = selected_source
    if float(asset_row.quality_score or 0.0) <= 0:
        asset_row.quality_score = 0.75 if selected_source == "steam" else 0.9
        changed = True
    if created:
        asset_row.version = int(asset_row.version or 0) + 1
    elif changed:
        asset_row.version = int(asset_row.version or 0) + 1
    asset_row.fetched_at = datetime.utcnow()
    return created, changed


def _ensure_cross_store_complete(
    db: Session,
    title: SteamTitle,
    steam_assets: Dict[str, Optional[str]],
) -> Tuple[bool, bool]:
    row = (
        db.query(CrossStoreMapping)
        .filter(CrossStoreMapping.steam_app_id == title.app_id)
        .order_by(CrossStoreMapping.confidence.desc(), CrossStoreMapping.updated_at.desc())
        .first()
    )
    created = False
    changed = False
    now_iso = datetime.utcnow().isoformat()
    fallback_product_id = f"steam-fallback-{title.app_id}"
    if row is None:
        row = CrossStoreMapping(
            steam_app_id=title.app_id,
            epic_product_id=fallback_product_id,
            confidence=CROSS_STORE_MAPPING_MIN_CONFIDENCE,
        )
        db.add(row)
        created = True
        changed = True

    score = float(row.confidence or 0.0)
    evidence = row.evidence if isinstance(row.evidence, dict) else {}
    verification_state = str(evidence.get("verification_state") or "").strip().lower()

    # Keep high-confidence matched mapping intact.
    if score >= CROSS_STORE_MAPPING_MIN_CONFIDENCE and verification_state == "matched":
        return created, changed

    row.confidence = max(score, CROSS_STORE_MAPPING_MIN_CONFIDENCE)
    row.epic_product_id = str(row.epic_product_id or "").strip() or fallback_product_id
    row.evidence = {
        **evidence,
        "steam_title": title.name,
        "verification_state": "fallback",
        "evidence_source": "steam_fallback",
        "assets": {
            "grid": steam_assets.get("grid"),
            "hero": steam_assets.get("hero"),
            "logo": steam_assets.get("logo"),
            "icon": steam_assets.get("icon"),
        },
        "updated_at": now_iso,
    }
    changed = True
    return created, changed


def enforce_catalog_completeness(
    db: Session,
    *,
    app_ids: Optional[List[str]] = None,
    max_items: Optional[int] = None,
) -> Dict[str, int]:
    ensure_global_index_schema()
    query = db.query(SteamTitle)
    app_id_filter_set: Optional[set[str]] = None
    if app_ids:
        normalized_ids = [str(app_id).strip() for app_id in app_ids if str(app_id).strip().isdigit()]
        if normalized_ids and len(normalized_ids) <= 900:
            query = query.filter(SteamTitle.app_id.in_(normalized_ids))
        elif normalized_ids:
            app_id_filter_set = set(normalized_ids)

    metadata_subq = select(SteamTitleMetadata.steam_title_id)
    assets_subq = select(SteamTitleAsset.steam_title_id)
    mapping_subq = select(CrossStoreMapping.steam_app_id).where(
        CrossStoreMapping.confidence >= CROSS_STORE_MAPPING_MIN_CONFIDENCE
    )
    query = query.order_by(
        case((SteamTitle.id.in_(metadata_subq), 1), else_=0).asc(),
        case((SteamTitle.id.in_(assets_subq), 1), else_=0).asc(),
        case((SteamTitle.app_id.in_(mapping_subq), 1), else_=0).asc(),
        desc(SteamTitle.updated_at),
        SteamTitle.app_id.asc(),
    )

    cap = max_items if isinstance(max_items, int) and max_items > 0 else STEAM_GLOBAL_INDEX_COMPLETION_BATCH
    if isinstance(cap, int) and cap > 0:
        query = query.limit(cap)

    stats = {
        "processed": 0,
        "failed": 0,
        "metadata_created": 0,
        "metadata_updated": 0,
        "assets_created": 0,
        "assets_updated": 0,
        "cross_store_created": 0,
        "cross_store_updated": 0,
    }
    titles = query.all()
    for index, title in enumerate(titles):
        if app_id_filter_set is not None and title.app_id not in app_id_filter_set:
            continue
        try:
            steam_assets = build_steam_fallback_assets(title.app_id)
            metadata_created, metadata_changed = _ensure_metadata_complete(db, title, steam_assets)
            assets_created, assets_changed = _ensure_assets_complete(db, title, steam_assets)
            mapping_created, mapping_changed = _ensure_cross_store_complete(db, title, steam_assets)

            if metadata_created:
                stats["metadata_created"] += 1
            if metadata_changed:
                stats["metadata_updated"] += 1
            if assets_created:
                stats["assets_created"] += 1
            if assets_changed:
                stats["assets_updated"] += 1
            if mapping_created:
                stats["cross_store_created"] += 1
            if mapping_changed:
                stats["cross_store_updated"] += 1
        except Exception:
            stats["failed"] += 1
        finally:
            stats["processed"] += 1
            if (index + 1) % 200 == 0:
                db.commit()

    db.commit()
    return stats


def _is_recent(timestamp: Optional[datetime], max_age_seconds: int) -> bool:
    if not timestamp:
        return False
    return (datetime.utcnow() - timestamp).total_seconds() < max_age_seconds


def enrich_external_catalog_data(
    db: Session,
    app_ids: List[str],
    *,
    force_refresh: bool = False,
) -> Dict[str, int]:
    ensure_global_index_schema()
    normalized = [str(app_id) for app_id in app_ids if str(app_id).strip().isdigit()]
    if not normalized:
        return {
            "steamdb_success": 0,
            "steamdb_failed": 0,
            "cross_store_success": 0,
            "cross_store_failed": 0,
        }

    if STEAMDB_ENRICHMENT_MAX_ITEMS > 0:
        normalized = normalized[:STEAMDB_ENRICHMENT_MAX_ITEMS]

    rows = (
        db.query(SteamTitle)
        .filter(SteamTitle.app_id.in_(normalized))
        .all()
    )
    by_app_id = {row.app_id: row for row in rows}
    epic_candidates = _build_epic_candidates(force_refresh=force_refresh)

    stats = {
        "steamdb_success": 0,
        "steamdb_failed": 0,
        "cross_store_success": 0,
        "cross_store_failed": 0,
    }

    for index, app_id in enumerate(normalized):
        title = by_app_id.get(app_id)
        if not title:
            continue

        if STEAMDB_ENRICHMENT_ENABLED:
            try:
                existing_enrichment = title.steamdb_row
                if (
                    existing_enrichment
                    and not force_refresh
                    and _is_recent(existing_enrichment.updated_at, 12 * 3600)
                ):
                    if float(existing_enrichment.confidence or 0.0) >= 0.35:
                        stats["steamdb_success"] += 1
                    else:
                        stats["steamdb_failed"] += 1
                else:
                    enrichment = _extract_steamdb_enrichment(app_id)
                    confidence = _upsert_steamdb_row(db, title, enrichment)
                    if confidence >= 0.35:
                        stats["steamdb_success"] += 1
                    else:
                        stats["steamdb_failed"] += 1
            except Exception:
                stats["steamdb_failed"] += 1

        if CROSS_STORE_MAPPING_ENABLED:
            try:
                existing_mapping = (
                    db.query(CrossStoreMapping)
                    .filter(CrossStoreMapping.steam_app_id == app_id)
                    .order_by(CrossStoreMapping.confidence.desc(), CrossStoreMapping.updated_at.desc())
                    .first()
                )
                if (
                    existing_mapping
                    and not force_refresh
                    and _is_recent(existing_mapping.updated_at, 6 * 3600)
                    and float(existing_mapping.confidence or 0.0) >= CROSS_STORE_MAPPING_MIN_CONFIDENCE
                ):
                    stats["cross_store_success"] += 1
                else:
                    matched = _select_cross_store_match(title, epic_candidates)
                    if matched:
                        score = _upsert_cross_store_mapping(db, title, matched)
                        if score >= CROSS_STORE_MAPPING_MIN_CONFIDENCE:
                            stats["cross_store_success"] += 1
                        else:
                            stats["cross_store_failed"] += 1
                    else:
                        stats["cross_store_failed"] += 1
            except Exception:
                stats["cross_store_failed"] += 1

        if (index + 1) % 50 == 0:
            db.commit()

    db.commit()
    return stats


def _fetch_steam_app_list_via_go_worker() -> List[Dict[str, Any]]:
    if not STEAM_GO_CRAWLER_ENABLED:
        return []

    worker = str(STEAM_GO_CRAWLER_BIN or "").strip()
    if not worker:
        return []
    worker_path = Path(worker)
    if not worker_path.exists():
        return []

    try:
        run = subprocess.run(
            [str(worker_path)],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(5, STEAM_GO_CRAWLER_TIMEOUT_SECONDS),
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if run.returncode != 0:
        return []

    try:
        payload = json.loads(run.stdout or "{}")
    except json.JSONDecodeError:
        return []
    raw_items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(raw_items, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        app_id = str(item.get("app_id") or "").strip()
        name = str(item.get("name") or "").strip()
        if not app_id.isdigit() or not name:
            continue
        normalized.append({"app_id": app_id, "name": name})
    return normalized


def fetch_steam_app_list() -> List[Dict[str, Any]]:
    via_go_worker = _fetch_steam_app_list_via_go_worker()
    if via_go_worker:
        return via_go_worker

    # Preferred source: IStoreService endpoint with pagination and API key.
    if STEAM_WEB_API_KEY:
        all_apps: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        last_appid = 0
        safety_pages = 500
        for _ in range(safety_pages):
            try:
                response = requests.get(
                    _steam_store_applist_url(),
                    params={
                        "key": STEAM_WEB_API_KEY,
                        "max_results": 50000,
                        "last_appid": last_appid,
                        "include_games": True,
                        "include_dlc": True,
                        "include_software": True,
                        "include_videos": False,
                        "include_hardware": False,
                    },
                    timeout=max(STEAM_REQUEST_TIMEOUT_SECONDS, 20),
                    headers={"User-Agent": "otoshi-launcher/1.0"},
                )
                if response.status_code != 200:
                    break
                payload = response.json()
            except (requests.RequestException, ValueError):
                break

            api_apps = ((payload or {}).get("response", {}) or {}).get("apps", [])
            if not isinstance(api_apps, list) or not api_apps:
                break

            page_added = 0
            for item in api_apps:
                if not isinstance(item, dict):
                    continue
                appid = item.get("appid")
                name = str(item.get("name") or "").strip()
                if not appid or not name:
                    continue
                app_id = str(appid)
                if app_id in seen_ids:
                    continue
                seen_ids.add(app_id)
                all_apps.append({"app_id": app_id, "name": name})
                page_added += 1

            response_obj = (payload or {}).get("response", {}) or {}
            have_more = bool(response_obj.get("have_more_results"))
            next_last_appid = int(response_obj.get("last_appid") or 0)
            if not have_more or next_last_appid <= 0 or page_added <= 0:
                break
            if next_last_appid <= last_appid:
                break
            last_appid = next_last_appid

        if all_apps:
            return all_apps

    # Legacy fallback: older ISteamApps endpoint.
    try:
        response = requests.get(
            _steam_applist_url(),
            timeout=STEAM_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "otoshi-launcher/1.0"},
        )
        if response.status_code != 200:
            return []
        payload = response.json()
    except (requests.RequestException, ValueError):
        return []

    apps = (
        (payload or {})
        .get("applist", {})
        .get("apps", [])
    )
    if not isinstance(apps, list):
        return []
    filtered: List[Dict[str, Any]] = []
    for item in apps:
        if not isinstance(item, dict):
            continue
        appid = item.get("appid")
        name = str(item.get("name") or "").strip()
        if not appid or not name:
            continue
        filtered.append({"app_id": str(appid), "name": name})
    return filtered


def _read_manifest_name_map() -> Dict[str, str]:
    if not _CHUNK_MANIFEST_MAP_FILE.exists():
        return {}
    try:
        payload = json.loads(_CHUNK_MANIFEST_MAP_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    steam_map = payload.get("steam_app_id") if isinstance(payload, dict) else None
    if not isinstance(steam_map, dict):
        return {}
    parsed: Dict[str, str] = {}
    for app_id, entry in steam_map.items():
        if not isinstance(entry, dict):
            continue
        raw_name = str(entry.get("game_name") or entry.get("folder") or "").strip()
        if raw_name:
            parsed[str(app_id)] = raw_name
    return parsed


def _fallback_apps_from_lua(max_items: Optional[int] = None) -> List[Dict[str, Any]]:
    appids = get_lua_appids()
    if max_items and max_items > 0:
        appids = appids[: max_items]
    if not appids:
        return []

    name_map = _read_manifest_name_map()
    allow_live_summary = bool(max_items and max_items <= 200)

    apps: List[Dict[str, Any]] = []
    for app_id in appids:
        name = name_map.get(str(app_id))
        if not name and allow_live_summary:
            summary = get_steam_summary(str(app_id)) or {}
            name = str(summary.get("name") or "").strip() or None
        apps.append(
            {
                "app_id": str(app_id),
                "name": name or f"Steam App {app_id}",
            }
        )
    return apps


def _resolve_ingest_seed_apps(max_items: Optional[int] = None) -> Tuple[List[Dict[str, Any]], str]:
    """Resolve the initial app list for ingest, with resilient Lua fallback retries."""
    apps = fetch_steam_app_list()
    if apps:
        return apps, "steam_api"

    # Startup can race with async Lua sync; poll fallback briefly before giving up.
    for attempt in range(max(1, int(_LUA_FALLBACK_SEED_RETRY_ATTEMPTS))):
        fallback_apps = _fallback_apps_from_lua(max_items=max_items)
        if fallback_apps:
            return fallback_apps, "lua_fallback"
        if attempt < _LUA_FALLBACK_SEED_RETRY_ATTEMPTS - 1:
            time.sleep(max(0, int(_LUA_FALLBACK_SEED_RETRY_DELAY_SECONDS)))

    return [], "lua_fallback"


def _upsert_alias(db: Session, title_id: str, alias: str, locale: str = "en", source: str = "steam") -> None:
    normalized = normalize_title(alias)
    if not normalized:
        return
    existing = (
        db.query(SteamTitleAlias)
        .filter(
            SteamTitleAlias.steam_title_id == title_id,
            SteamTitleAlias.normalized_alias == normalized,
            SteamTitleAlias.locale == locale,
        )
        .first()
    )
    if existing:
        existing.alias = alias
        existing.source = source
        return
    db.add(
        SteamTitleAlias(
            steam_title_id=title_id,
            alias=alias,
            normalized_alias=normalized,
            locale=locale,
            source=source,
        )
    )


def _ensure_title_row(db: Session, app_id: str, name: str, source: str = "steam_api") -> SteamTitle:
    row = db.query(SteamTitle).filter(SteamTitle.app_id == str(app_id)).first()
    app_id_text = str(app_id)
    incoming_name = _pick_best_title_name(app_id_text, name)
    normalized_name = normalize_title(incoming_name)
    if row:
        existing_name = str(row.name or "").strip()
        incoming_placeholder = _is_placeholder_title_name(incoming_name, app_id_text)
        existing_placeholder = _is_placeholder_title_name(existing_name, app_id_text)
        should_update_name = (not incoming_placeholder) or existing_placeholder
        if should_update_name:
            row.name = incoming_name
            row.normalized_name = normalized_name
            _upsert_alias(db, row.id, incoming_name, locale="en", source=source)
        row.source = source
        row.updated_at = datetime.utcnow()
    else:
        row = SteamTitle(
            app_id=app_id_text,
            name=incoming_name,
            normalized_name=normalized_name,
            source=source,
            state="active",
        )
        db.add(row)
        db.flush()
        _upsert_alias(db, row.id, incoming_name, locale="en", source=source)
    return row


def _upsert_metadata_row(
    db: Session,
    title: SteamTitle,
    summary: Optional[Dict[str, Any]],
    detail: Optional[Dict[str, Any]],
) -> SteamTitleMetadata:
    metadata = (
        db.query(SteamTitleMetadata)
        .filter(SteamTitleMetadata.steam_title_id == title.id)
        .first()
    )
    summary = summary or {}
    detail = detail or {}
    if metadata is None:
        metadata = SteamTitleMetadata(steam_title_id=title.id)
        db.add(metadata)
    metadata.short_description = (
        detail.get("short_description")
        or summary.get("short_description")
        or metadata.short_description
    )
    metadata.long_description = (
        detail.get("detailed_description")
        or detail.get("about_the_game")
        or metadata.long_description
    )
    metadata.genres = detail.get("genres") or summary.get("genres") or metadata.genres or []
    metadata.platforms = detail.get("platforms") or summary.get("platforms") or metadata.platforms or []
    metadata.requirements = detail.get("pc_requirements") or metadata.requirements or {}
    metadata.reviews = detail.get("reviews") or metadata.reviews or {}
    metadata.players = detail.get("players") or metadata.players or {}
    metadata.dlc_graph = detail.get("dlc_graph") or metadata.dlc_graph or {}
    metadata.summary_payload = summary or {}
    metadata.detail_payload = detail or {}
    metadata.last_refreshed_at = datetime.utcnow()
    return metadata


def refresh_title_from_steam(db: Session, app_id: str) -> Optional[SteamTitle]:
    summary = get_steam_summary(str(app_id))
    detail = get_steam_detail(str(app_id))
    if not summary and not detail:
        return None
    name = (
        (detail or {}).get("name")
        or (summary or {}).get("name")
        or f"Steam App {app_id}"
    )
    title = _ensure_title_row(db, str(app_id), name, source="steam_api")
    item_type = str(
        (detail or {}).get("item_type")
        or (detail or {}).get("type")
        or (summary or {}).get("item_type")
        or (summary or {}).get("type")
        or title.title_type
        or ""
    ).strip().lower()
    title.title_type = item_type or title.title_type
    title.release_date = (detail or {}).get("release_date") or (summary or {}).get("release_date")
    title.developer = _pick_first_string((detail or {}).get("developers"))
    title.publisher = _pick_first_string((detail or {}).get("publishers"))
    title.platform_flags = _build_platform_flags((detail or {}).get("platforms") or (summary or {}).get("platforms"))
    _upsert_metadata_row(db, title, summary, detail)
    return title


def _build_platform_flags(platforms: Any) -> Dict[str, bool]:
    flags = {"windows": False, "mac": False, "linux": False}
    if isinstance(platforms, dict):
        for key in flags.keys():
            flags[key] = bool(platforms.get(key))
        return flags
    if isinstance(platforms, list):
        lower = {str(item).lower() for item in platforms}
        for key in flags.keys():
            flags[key] = key in lower
    return flags


def _pick_first_string(values: Any) -> Optional[str]:
    if isinstance(values, list):
        for item in values:
            text = str(item or "").strip()
            if text:
                return text
    if isinstance(values, str) and values.strip():
        return values.strip()
    return None


def ingest_global_catalog(
    db: Session,
    max_items: Optional[int] = None,
    enrich_details: bool = True,
) -> Dict[str, Any]:
    ensure_global_index_schema()
    started = datetime.utcnow()
    job = IngestJob(
        job_type="steam_global_catalog",
        status="running",
        source="steam_api",
        started_at=started,
        meta={"max_items": max_items, "enrich_details": enrich_details},
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    apps, source = _resolve_ingest_seed_apps(max_items=max_items)
    job.source = source
    db.commit()
    if max_items and max_items > 0:
        apps = apps[: max_items]

    processed = 0
    created_or_updated = 0
    failed = 0
    appids_for_detail: List[str] = []
    external_stats = {
        "steamdb_success": 0,
        "steamdb_failed": 0,
        "cross_store_success": 0,
        "cross_store_failed": 0,
        "completion_processed": 0,
        "completion_failed": 0,
        "completion_metadata_created": 0,
        "completion_assets_created": 0,
        "completion_cross_store_created": 0,
    }

    try:
        batch_size = max(10, STEAM_GLOBAL_INDEX_INGEST_BATCH)
        for start in range(0, len(apps), batch_size):
            batch = apps[start : start + batch_size]
            ids = [entry["app_id"] for entry in batch]
            existing = {
                row.app_id: row
                for row in db.query(SteamTitle).filter(SteamTitle.app_id.in_(ids)).all()
            }
            for entry in batch:
                app_id = entry["app_id"]
                name = entry["name"]
                try:
                    row = existing.get(app_id)
                    if row:
                        row.name = name
                        row.normalized_name = normalize_title(name)
                        row.state = "active"
                        row.source = source
                        _upsert_alias(db, row.id, name, locale="en", source=source)
                    else:
                        row = _ensure_title_row(db, app_id, name, source=source)
                    created_or_updated += 1
                    appids_for_detail.append(app_id)
                except Exception:
                    failed += 1
                processed += 1
            db.commit()

        detail_limit = min(len(appids_for_detail), max_items or len(appids_for_detail))
        if enrich_details:
            for app_id in appids_for_detail[:detail_limit]:
                try:
                    refresh_title_from_steam(db, app_id)
                except Exception:
                    failed += 1
            db.commit()
            external_stats = enrich_external_catalog_data(
                db,
                app_ids=appids_for_detail[:detail_limit],
                force_refresh=False,
            )

        if STEAM_GLOBAL_INDEX_ENFORCE_COMPLETE:
            completion_ids = appids_for_detail[:detail_limit] if appids_for_detail else None
            completion_stats = enforce_catalog_completeness(
                db,
                app_ids=completion_ids,
                max_items=max_items,
            )
            external_stats.update(
                {
                    "completion_processed": int(completion_stats.get("processed") or 0),
                    "completion_failed": int(completion_stats.get("failed") or 0),
                    "completion_metadata_created": int(completion_stats.get("metadata_created") or 0),
                    "completion_assets_created": int(completion_stats.get("assets_created") or 0),
                    "completion_cross_store_created": int(completion_stats.get("cross_store_created") or 0),
                }
            )

        job.meta = {
            **(job.meta or {}),
            "source": source,
            "external_enrichment": external_stats,
        }

        job.status = "completed"
        job.processed_count = processed
        job.success_count = created_or_updated
        job.failure_count = failed
        job.completed_at = datetime.utcnow()
        db.commit()
    except Exception as exc:
        db.rollback()
        job.status = "failed"
        job.error_message = str(exc)
        job.processed_count = processed
        job.success_count = created_or_updated
        job.failure_count = failed + 1
        job.completed_at = datetime.utcnow()
        db.commit()
        raise

    return {
        "job_id": job.id,
        "processed": processed,
        "success": created_or_updated,
        "failed": failed,
        "steamdb_success": external_stats["steamdb_success"],
        "steamdb_failed": external_stats["steamdb_failed"],
        "cross_store_success": external_stats["cross_store_success"],
        "cross_store_failed": external_stats["cross_store_failed"],
        "completion_processed": external_stats.get("completion_processed", 0),
        "completion_failed": external_stats.get("completion_failed", 0),
        "started_at": started.isoformat(),
        "completed_at": (job.completed_at or datetime.utcnow()).isoformat(),
    }


def _infer_artwork_coverage(source: Optional[str]) -> str:
    normalized = str(source or "").strip().lower()
    if normalized in {"steamgriddb", "sgdb"}:
        return "sgdb"
    if normalized in {"epic", "epic_mapped"}:
        return "epic"
    if normalized in {"mixed"}:
        return "mixed"
    return "steam"


def _build_catalog_item(
    title: SteamTitle,
    manifest_name_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    metadata = title.metadata_row
    detail_payload = (metadata.detail_payload if metadata else {}) or {}
    summary_payload = (metadata.summary_payload if metadata else {}) or {}
    selected_assets = (title.assets_row.selected_assets if title.assets_row else {}) or {}
    fallback_assets = build_steam_fallback_assets(title.app_id)
    manifest_name = None
    if manifest_name_map:
        manifest_name = manifest_name_map.get(str(title.app_id))
    resolved_name = _pick_best_title_name(
        str(title.app_id),
        detail_payload.get("name"),
        summary_payload.get("name"),
        title.name,
        manifest_name,
    )

    header = (
        selected_assets.get("grid")
        or detail_payload.get("header_image")
        or summary_payload.get("header_image")
        or fallback_assets.get("grid")
    )
    capsule = (
        selected_assets.get("grid")
        or detail_payload.get("capsule_image")
        or summary_payload.get("capsule_image")
        or fallback_assets.get("grid")
    )
    background = (
        selected_assets.get("hero")
        or detail_payload.get("background")
        or summary_payload.get("background")
        or fallback_assets.get("hero")
    )
    denuvo_flag = bool(detail_payload.get("denuvo") or summary_payload.get("denuvo"))
    if str(title.app_id) in DENUVO_APP_ID_SET:
        denuvo_flag = True
    item_type = str(
        detail_payload.get("item_type")
        or detail_payload.get("type")
        or summary_payload.get("item_type")
        or summary_payload.get("type")
        or title.title_type
        or ""
    ).strip().lower()
    is_dlc = item_type == "dlc"
    if not is_dlc and _DLC_HINTS.search(resolved_name or ""):
        is_dlc = True
    is_base_game = not is_dlc
    classification_confidence = 0.62
    if item_type:
        classification_confidence = 0.97 if item_type in {"game", "dlc"} else 0.88
    elif _DLC_HINTS.search(resolved_name or ""):
        classification_confidence = 0.83

    return {
        "app_id": title.app_id,
        "name": resolved_name,
        "short_description": (
            detail_payload.get("short_description")
            or summary_payload.get("short_description")
            or (metadata.short_description if metadata else None)
        ),
        "header_image": header,
        "capsule_image": capsule,
        "background": background,
        "required_age": detail_payload.get("required_age") or summary_payload.get("required_age"),
        "price": detail_payload.get("price") or summary_payload.get("price"),
        "genres": detail_payload.get("genres") or summary_payload.get("genres") or [],
        "release_date": title.release_date or detail_payload.get("release_date") or summary_payload.get("release_date"),
        "platforms": detail_payload.get("platforms") or summary_payload.get("platforms") or [],
        "item_type": item_type or None,
        "is_dlc": is_dlc,
        "is_base_game": is_base_game,
        "classification_confidence": classification_confidence,
        "artwork_coverage": _infer_artwork_coverage(title.assets_row.selected_source if title.assets_row else None),
        "denuvo": denuvo_flag,
        "artwork": {
            "t0": (
                selected_assets.get("icon")
                or selected_assets.get("logo")
                or selected_assets.get("grid")
                or selected_assets.get("hero")
                or fallback_assets.get("icon")
                or fallback_assets.get("grid")
                or fallback_assets.get("hero")
            ),
            "t1": capsule,
            "t2": capsule,
            "t3": header,
            "t4": background,
            "version": int(title.assets_row.version) if title.assets_row else 1,
        },
    }


def list_catalog(
    db: Session,
    limit: int,
    offset: int,
    sort: Optional[str] = None,
    scope: str = "all",
    library_appids: Optional[Iterable[str]] = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    def _run():
        manifest_name_map = _read_manifest_name_map()
        query = db.query(SteamTitle)
        scope_set: Optional[set[str]] = None
        if scope in {"library", "owned"}:
            appids = [str(value) for value in (library_appids or []) if str(value).strip()]
            if not appids:
                return 0, []
            scope_set = set(appids)
            query = query.filter(SteamTitle.app_id.in_(appids))

        sort_value = (sort or "name").lower()
        if sort_value in {"priority", "top", "top_picks", "hot"}:
            total = query.count()
            if total <= 0:
                return 0, []

            candidates = _build_priority_candidate_appids(scope_set)
            candidate_unique: list[str] = []
            if candidates:
                seen_candidates: set[str] = set()
                for app_id in candidates:
                    if app_id in seen_candidates:
                        continue
                    seen_candidates.add(app_id)
                    candidate_unique.append(app_id)

            priority_ids: list[str] = []
            if candidate_unique:
                # Only keep candidates that exist in current scope.
                existing_rows = (
                    query.filter(SteamTitle.app_id.in_(candidate_unique))
                    .with_entities(SteamTitle.app_id)
                    .all()
                )
                existing = {row[0] for row in existing_rows if row and row[0]}
                seen: set[str] = set()
                for app_id in candidate_unique:
                    if app_id in existing and app_id not in seen:
                        priority_ids.append(app_id)
                        seen.add(app_id)

            priority_total = len(priority_ids)
            page_rows: list[SteamTitle] = []
            remaining_limit = max(1, limit)
            offset_value = max(0, offset)

            def rest_query():
                if not priority_ids:
                    return query
                return query.filter(~SteamTitle.app_id.in_(priority_ids))

            if offset_value >= priority_total:
                rest_offset = offset_value - priority_total
                page_rows = (
                    rest_query()
                    .order_by(desc(SteamTitle.updated_at), SteamTitle.name.asc())
                    .offset(rest_offset)
                    .limit(remaining_limit)
                    .all()
                )
            else:
                slice_ids = priority_ids[offset_value : offset_value + remaining_limit]
                if slice_ids:
                    fetched = query.filter(SteamTitle.app_id.in_(slice_ids)).all()
                    row_by_id = {row.app_id: row for row in fetched}
                    ordered = [row_by_id[app_id] for app_id in slice_ids if app_id in row_by_id]
                    page_rows.extend(ordered)
                    remaining_limit = remaining_limit - len(ordered)

                if remaining_limit > 0:
                    rest_rows = (
                        rest_query()
                        .order_by(desc(SteamTitle.updated_at), SteamTitle.name.asc())
                        .offset(0)
                        .limit(remaining_limit)
                        .all()
                    )
                    page_rows.extend(rest_rows)

            return total, [_build_catalog_item(row, manifest_name_map) for row in page_rows]
        if sort_value in {"recent", "updated"}:
            query = query.order_by(desc(SteamTitle.updated_at), SteamTitle.name.asc())
        elif sort_value == "appid":
            query = query.order_by(SteamTitle.app_id.asc())
        else:
            query = query.order_by(SteamTitle.name.asc())

        total = query.count()
        rows = query.offset(max(0, offset)).limit(max(1, limit)).all()
        return total, [_build_catalog_item(row, manifest_name_map) for row in rows]

    return _with_schema_retry(_run)


def search_catalog(
    db: Session,
    q: str,
    limit: int,
    offset: int,
    include_dlc: Optional[bool] = None,
    ranking_mode: Optional[str] = None,
    must_have_artwork: bool = False,
) -> Tuple[int, List[Dict[str, Any]]]:
    def _run():
        manifest_name_map = _read_manifest_name_map()
        query = (q or "").strip()
        if not query:
            return 0, []

        max_limit = min(max(1, limit), max(1, STEAM_GLOBAL_INDEX_SEARCH_LIMIT))
        normalized = normalize_title(query)
        compact_query = _compact_alnum(query)
        letters_only, digits_only = _split_compact_query(query)
        initials_pattern = ""
        if " " not in query.strip() and compact_query and 2 <= len(letters_only) <= 8:
            initials_pattern = _build_initials_like_pattern(letters_only)

        base = db.query(SteamTitle)
        if query.isdigit():
            exact = base.filter(SteamTitle.app_id == query).first()
            if exact:
                return 1, [_build_catalog_item(exact, manifest_name_map)]

        alias_subq = select(SteamTitleAlias.steam_title_id).where(
            SteamTitleAlias.normalized_alias.ilike(f"%{normalized}%")
        )

        lowered_query = query.lower()
        noise_patterns = [
            "% soundtrack%",
            "% dlc%",
            "% demo%",
            "% beta%",
            "% test server%",
            "% pack%",
            "% bundle%",
            "% set%",
            "% edition upgrade%",
            "% costume%",
            "% mission%",
            "% bonus%",
            "% starter%",
            "% pachislot%",
        ]
        name_length = func.length(SteamTitle.normalized_name)
        noise_penalty = case(
            *[(SteamTitle.normalized_name.ilike(pattern), 1) for pattern in noise_patterns],
            else_=0,
        )
        compact_name = func.replace(SteamTitle.normalized_name, " ", "")

        relevance_conditions = [
            (SteamTitle.app_id == query, 900),
            (func.lower(SteamTitle.name) == lowered_query, 800),
            (SteamTitle.normalized_name == normalized, 780),
            (func.lower(SteamTitle.name).ilike(f"{lowered_query}:%"), 760),
            (SteamTitle.normalized_name.ilike(f"{normalized} %"), 740),
            (SteamTitle.normalized_name.ilike(f"{normalized}%"), 720),
            (SteamTitle.app_id.ilike(f"{query}%"), 700),
            (SteamTitle.id.in_(alias_subq), 520),
            (SteamTitle.normalized_name.ilike(f"%{normalized}%"), 500),
        ]
        if compact_query and compact_query != normalized:
            relevance_conditions.extend(
                [
                    (compact_name == compact_query, 750),
                    (compact_name.ilike(f"{compact_query}%"), 735),
                    (compact_name.ilike(f"%{compact_query}%"), 680),
                ]
            )
        if initials_pattern:
            relevance_conditions.extend(
                [
                    (SteamTitle.normalized_name.ilike(initials_pattern), 730),
                    (SteamTitle.normalized_name.ilike(f"% {initials_pattern}"), 720),
                ]
            )
            if digits_only:
                relevance_conditions.extend(
                    [
                        (
                            and_(
                                SteamTitle.normalized_name.ilike(initials_pattern),
                                SteamTitle.normalized_name.ilike(f"%{digits_only}%"),
                            ),
                            740,
                        ),
                        (
                            and_(
                                SteamTitle.normalized_name.ilike(f"% {initials_pattern}"),
                                SteamTitle.normalized_name.ilike(f"%{digits_only}%"),
                            ),
                            735,
                        ),
                    ]
                )
        relevance_score = case(*relevance_conditions, else_=0)

        match_conditions = [
            SteamTitle.normalized_name.ilike(f"%{normalized}%"),
            SteamTitle.app_id.ilike(f"{query}%"),
            SteamTitle.id.in_(alias_subq),
        ]
        if compact_query and compact_query != normalized:
            match_conditions.append(compact_name.ilike(f"%{compact_query}%"))
        if initials_pattern:
            initials_match = or_(
                SteamTitle.normalized_name.ilike(initials_pattern),
                SteamTitle.normalized_name.ilike(f"% {initials_pattern}"),
            )
            if digits_only:
                initials_match = or_(
                    and_(SteamTitle.normalized_name.ilike(initials_pattern), SteamTitle.normalized_name.ilike(f"%{digits_only}%")),
                    and_(SteamTitle.normalized_name.ilike(f"% {initials_pattern}"), SteamTitle.normalized_name.ilike(f"%{digits_only}%")),
                )
            match_conditions.append(initials_match)

        rows_query = base.filter(or_(*match_conditions))

        if include_dlc is False:
            rows_query = rows_query.filter(
                ~or_(
                    SteamTitle.title_type == "dlc",
                    SteamTitle.normalized_name.ilike("% dlc%"),
                    SteamTitle.normalized_name.ilike("% soundtrack%"),
                    SteamTitle.normalized_name.ilike("% season pass%"),
                    SteamTitle.normalized_name.ilike("% expansion%"),
                    SteamTitle.normalized_name.ilike("% costume%"),
                    SteamTitle.normalized_name.ilike("% bonus%"),
                    SteamTitle.normalized_name.ilike("% mission%"),
                    SteamTitle.normalized_name.ilike("% set%"),
                    SteamTitle.normalized_name.ilike("% pack%"),
                    SteamTitle.normalized_name.ilike("% pachislot%"),
                )
            )

        if must_have_artwork:
            artwork_subq = select(SteamTitleAsset.steam_title_id).where(
                SteamTitleAsset.selected_assets.isnot(None)
            )
            rows_query = rows_query.filter(SteamTitle.id.in_(artwork_subq))

        rank_mode = str(ranking_mode or "").strip().lower()
        priority_ids: List[str] = []
        if rank_mode in {"hot", "priority", "top"}:
            seen: set[str] = set()
            for app_id in _build_priority_candidate_appids():
                app_str = str(app_id).strip()
                if not app_str:
                    continue
                if app_str not in seen:
                    priority_ids.append(app_str)
                    seen.add(app_str)
        priority_rank = case(
            (SteamTitle.app_id.in_(priority_ids), 1),
            else_=0,
        )

        if rank_mode in {"recent", "updated"}:
            rows_query = rows_query.order_by(
                priority_rank.desc(),
                SteamTitle.updated_at.desc(),
                relevance_score.desc(),
                noise_penalty.asc(),
                name_length.asc(),
                SteamTitle.name.asc(),
            )
        else:
            rows_query = rows_query.order_by(
                priority_rank.desc(),
                relevance_score.desc(),
                noise_penalty.asc(),
                name_length.asc(),
                SteamTitle.updated_at.desc(),
                SteamTitle.name.asc(),
            )

        total = rows_query.count()
        rows = rows_query.offset(max(0, offset)).limit(max_limit).all()
        return total, [_build_catalog_item(row, manifest_name_map) for row in rows]

    return _with_schema_retry(_run)


def get_title_detail(db: Session, app_id: str) -> Optional[Dict[str, Any]]:
    def _run():
        manifest_name_map = _read_manifest_name_map()
        title = db.query(SteamTitle).filter(SteamTitle.app_id == str(app_id)).first()
        if not title:
            refreshed = refresh_title_from_steam(db, app_id)
            if refreshed:
                db.commit()
                db.refresh(refreshed)
                title = refreshed
        if not title:
            return None

        metadata = title.metadata_row
        detail = (metadata.detail_payload if metadata else {}) or {}
        if not detail:
            detail = get_steam_detail(str(app_id)) or {}
            if detail:
                _upsert_metadata_row(db, title, get_steam_summary(str(app_id)) or {}, detail)
                db.commit()

        if not detail:
            summary = _build_catalog_item(title, manifest_name_map)
            return {
                **summary,
                "about_the_game": summary.get("short_description"),
                "about_the_game_html": None,
                "detailed_description": summary.get("short_description"),
                "detailed_description_html": None,
                "developers": [title.developer] if title.developer else [],
                "publishers": [title.publisher] if title.publisher else [],
                "categories": [],
                "screenshots": [value for value in [summary.get("header_image"), summary.get("background")] if value],
                "movies": [],
                "pc_requirements": {},
                "metacritic": None,
                "recommendations": None,
                "website": None,
                "support_info": None,
            }
        return detail

    return _with_schema_retry(_run)


def _resolve_epic_assets(
    db: Session,
    app_id: str,
) -> Dict[str, Optional[str]]:
    mapping = (
        db.query(CrossStoreMapping)
        .filter(CrossStoreMapping.steam_app_id == str(app_id))
        .order_by(CrossStoreMapping.confidence.desc())
        .first()
    )
    if not mapping:
        return {}
    if float(mapping.confidence or 0.0) < STEAM_GLOBAL_INDEX_EPIC_CONFIDENCE_THRESHOLD:
        return {}
    evidence = mapping.evidence if isinstance(mapping.evidence, dict) else {}
    evidence_assets = evidence.get("assets") if isinstance(evidence.get("assets"), dict) else evidence
    return {
        "grid": evidence_assets.get("grid"),
        "hero": evidence_assets.get("hero"),
        "logo": evidence_assets.get("logo"),
        "icon": evidence_assets.get("icon"),
    }


def _choose_assets_source(
    sgdb_assets: Dict[str, Optional[str]],
    epic_assets: Dict[str, Optional[str]],
    steam_assets: Dict[str, Optional[str]],
) -> Tuple[str, Dict[str, Optional[str]], float]:
    if sgdb_assets.get("grid") or sgdb_assets.get("hero"):
        return "steamgriddb", sgdb_assets, 1.0
    if epic_assets.get("grid") or epic_assets.get("hero"):
        return "epic", epic_assets, 0.92
    return "steam", steam_assets, 0.75


def _normalize_selected_assets(
    app_id: str,
    selected_assets: Dict[str, Optional[str]],
    steam_assets: Dict[str, Optional[str]],
) -> Dict[str, Optional[str]]:
    grid = (
        selected_assets.get("grid")
        or selected_assets.get("hero")
        or steam_assets.get("grid")
        or steam_assets.get("hero")
    )
    hero = (
        selected_assets.get("hero")
        or selected_assets.get("grid")
        or steam_assets.get("hero")
        or steam_assets.get("grid")
    )
    logo = (
        selected_assets.get("logo")
        or selected_assets.get("icon")
        or selected_assets.get("grid")
        or steam_assets.get("logo")
        or steam_assets.get("icon")
    )
    raw_icon = selected_assets.get("icon")
    if raw_icon:
        marker = f"/steam/apps/{app_id}/icon.jpg"
        lower_icon = raw_icon.lower()
        if marker in lower_icon and any(
            value and "steamgriddb.com" in value.lower()
            for value in (selected_assets.get("logo"), selected_assets.get("grid"), selected_assets.get("hero"))
        ):
            raw_icon = None
    icon = (
        raw_icon
        or selected_assets.get("logo")
        or selected_assets.get("grid")
        or selected_assets.get("hero")
        or steam_assets.get("icon")
        or steam_assets.get("grid")
    )
    return {
        "grid": grid,
        "hero": hero,
        "logo": logo,
        "icon": icon,
    }


def resolve_assets_chain(
    db: Session,
    app_id: str,
    title_hint: Optional[str] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    ensure_global_index_schema()
    app_id = str(app_id)
    title = db.query(SteamTitle).filter(SteamTitle.app_id == app_id).first()
    if title is None:
        title = refresh_title_from_steam(db, app_id)
        if title:
            db.commit()
    if title is None:
        return {"app_id": app_id, "selected_source": "steam", "assets": build_steam_fallback_assets(app_id)}

    if title.assets_row and not force_refresh:
        cached_assets = (title.assets_row.selected_assets or {}) if isinstance(title.assets_row.selected_assets, dict) else {}
        steam_assets = (
            (title.assets_row.steam_assets or {})
            if isinstance(title.assets_row.steam_assets, dict)
            else build_steam_fallback_assets(app_id)
        )
        normalized_cached_assets = _normalize_selected_assets(app_id, cached_assets, steam_assets)
        if normalized_cached_assets != cached_assets:
            title.assets_row.selected_assets = normalized_cached_assets
            title.assets_row.version = int(title.assets_row.version or 0) + 1
            title.assets_row.updated_at = datetime.utcnow()
            db.commit()
            db.refresh(title.assets_row)
        return {
            "app_id": app_id,
            "selected_source": title.assets_row.selected_source,
            "assets": title.assets_row.selected_assets or {},
            "quality_score": float(title.assets_row.quality_score or 0.0),
            "version": int(title.assets_row.version or 1),
        }

    search_title = title_hint or title.name
    sgdb = resolve_assets(app_id, search_title)
    sgdb_assets = {
        "grid": sgdb.get("grid"),
        "hero": sgdb.get("hero"),
        "logo": sgdb.get("logo"),
        "icon": sgdb.get("icon"),
    }
    epic_assets = _resolve_epic_assets(db, app_id)
    steam_assets = build_steam_fallback_assets(app_id)

    selected_source, selected_assets, score = _choose_assets_source(
        sgdb_assets, epic_assets, steam_assets
    )
    normalized_selected_assets = _normalize_selected_assets(app_id, selected_assets, steam_assets)

    asset_row = title.assets_row
    if not asset_row:
        asset_row = SteamTitleAsset(steam_title_id=title.id)
        db.add(asset_row)
    asset_row.sgdb_assets = sgdb_assets
    asset_row.epic_assets = epic_assets
    asset_row.steam_assets = steam_assets
    asset_row.selected_assets = normalized_selected_assets
    asset_row.selected_source = selected_source
    asset_row.quality_score = score
    asset_row.version = int(asset_row.version or 0) + 1
    asset_row.fetched_at = datetime.utcnow()
    db.commit()
    db.refresh(asset_row)

    return {
        "app_id": app_id,
        "selected_source": selected_source,
        "assets": normalized_selected_assets,
        "quality_score": score,
        "version": asset_row.version,
    }


def prefetch_assets(
    db: Session,
    app_ids: List[str],
    force_refresh: bool = False,
) -> Dict[str, Any]:
    ensure_global_index_schema()
    normalized_ids = [str(app_id).strip() for app_id in app_ids if str(app_id).strip().isdigit()]
    normalized_ids = normalized_ids[: max(1, STEAM_GLOBAL_INDEX_MAX_PREFETCH)]
    if not normalized_ids:
        return {"total": 0, "processed": 0, "success": 0, "failed": 0}

    success = 0
    failed = 0

    for index, app_id in enumerate(normalized_ids):
        job = AssetJob(app_id=app_id, status="running", priority=index + 1)
        db.add(job)
        db.commit()
        db.refresh(job)
        try:
            result = resolve_assets_chain(db, app_id, force_refresh=force_refresh)
            job.status = "completed"
            job.result_source = result.get("selected_source")
            job.result_meta = {"version": result.get("version"), "quality_score": result.get("quality_score")}
            success += 1
        except Exception as exc:
            job.status = "failed"
            job.last_error = str(exc)
            job.retries = int(job.retries or 0) + 1
            failed += 1
        db.commit()

    return {
        "total": len(normalized_ids),
        "processed": len(normalized_ids),
        "success": success,
        "failed": failed,
    }


def prefetch_assets_force_visible(
    db: Session,
    app_ids: List[str],
) -> Dict[str, Any]:
    return prefetch_assets(db=db, app_ids=app_ids, force_refresh=True)


def get_title_classification(db: Session, app_id: str) -> Dict[str, Any]:
    ensure_global_index_schema()
    app_id = str(app_id)
    title = db.query(SteamTitle).filter(SteamTitle.app_id == app_id).first()
    if title is None:
        title = refresh_title_from_steam(db, app_id)
        if title is not None:
            db.commit()

    if title is None:
        return {
            "app_id": app_id,
            "item_type": None,
            "is_dlc": False,
            "is_base_game": True,
            "confidence": 0.0,
        }

    metadata = title.metadata_row
    detail_payload = (metadata.detail_payload if metadata else {}) or {}
    summary_payload = (metadata.summary_payload if metadata else {}) or {}
    item_type = str(
        detail_payload.get("item_type")
        or detail_payload.get("type")
        or summary_payload.get("item_type")
        or summary_payload.get("type")
        or title.title_type
        or ""
    ).strip().lower()
    is_dlc = item_type == "dlc"
    if not is_dlc and _DLC_HINTS.search(title.name or ""):
        is_dlc = True
    confidence = 0.58
    if item_type:
        confidence = 0.96 if item_type in {"game", "dlc"} else 0.86
    elif _DLC_HINTS.search(title.name or ""):
        confidence = 0.82
    return {
        "app_id": app_id,
        "item_type": item_type or None,
        "is_dlc": is_dlc,
        "is_base_game": not is_dlc,
        "confidence": confidence,
    }


def get_catalog_coverage(db: Session) -> Dict[str, int]:
    ensure_global_index_schema()

    titles_total = int(db.query(func.count(SteamTitle.id)).scalar() or 0)
    metadata_complete = int(
        db.query(func.count(SteamTitleMetadata.id)).scalar() or 0
    )
    assets_complete = int(
        db.query(func.count(SteamTitleAsset.id)).scalar() or 0
    )
    cross_store_complete = int(
        db.query(func.count(CrossStoreMapping.id)).scalar() or 0
    )

    metadata_subq = select(SteamTitleMetadata.steam_title_id)
    assets_subq = select(SteamTitleAsset.steam_title_id)
    mapped_appids_subq = select(CrossStoreMapping.steam_app_id).where(
        CrossStoreMapping.confidence >= CROSS_STORE_MAPPING_MIN_CONFIDENCE
    )
    absolute_complete = int(
        db.query(func.count(SteamTitle.id))
        .filter(SteamTitle.id.in_(metadata_subq))
        .filter(SteamTitle.id.in_(assets_subq))
        .filter(SteamTitle.app_id.in_(mapped_appids_subq))
        .scalar()
        or 0
    )

    return {
        "titles_total": titles_total,
        "metadata_complete": metadata_complete,
        "assets_complete": assets_complete,
        "cross_store_complete": cross_store_complete,
        "absolute_complete": absolute_complete,
    }


def ingest_full_catalog(
    db: Session,
    *,
    max_items: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_global_index_schema()
    resume_token = f"full:{int(time.time())}"
    cursor = (
        db.query(IngestCursor)
        .filter(IngestCursor.cursor_key == "steam_global_catalog")
        .first()
    )
    if cursor is None:
        cursor = IngestCursor(cursor_key="steam_global_catalog")
        db.add(cursor)
    cursor.cursor_value = resume_token
    cursor.cursor_meta = {
        "phase": "full",
        "updated_at": datetime.utcnow().isoformat(),
    }
    db.commit()

    result = ingest_global_catalog(db=db, max_items=max_items, enrich_details=True)
    latest = (
        db.query(IngestJob)
        .filter(IngestJob.id == result.get("job_id"))
        .first()
    )
    if latest is not None:
        meta = latest.meta if isinstance(latest.meta, dict) else {}
        meta.update(
            {
                "phase": "full",
                "cursor": cursor.cursor_value,
                "resume_token": resume_token,
            }
        )
        latest.meta = meta
        db.commit()
    result["resume_token"] = resume_token
    return result


def resume_ingest_catalog(
    db: Session,
    *,
    resume_token: Optional[str] = None,
    max_items: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_global_index_schema()
    cursor = (
        db.query(IngestCursor)
        .filter(IngestCursor.cursor_key == "steam_global_catalog")
        .first()
    )
    effective_token = str(resume_token or (cursor.cursor_value if cursor else "") or "").strip()
    result = ingest_global_catalog(db=db, max_items=max_items, enrich_details=True)
    latest = (
        db.query(IngestJob)
        .filter(IngestJob.id == result.get("job_id"))
        .first()
    )
    if latest is not None:
        meta = latest.meta if isinstance(latest.meta, dict) else {}
        meta.update(
            {
                "phase": "resume",
                "cursor": effective_token or None,
                "resume_token": effective_token or None,
            }
        )
        latest.meta = meta
        db.commit()
    result["resumed_from"] = effective_token or None
    return result


def list_top_ranked(
    db: Session,
    *,
    limit: int,
    offset: int = 0,
    include_dlc: bool = False,
) -> Tuple[int, List[Dict[str, Any]]]:
    total, items = list_catalog(
        db=db,
        limit=limit,
        offset=offset,
        sort="priority",
        scope="all",
    )
    if include_dlc:
        return total, items
    filtered = [item for item in items if not bool(item.get("is_dlc"))]
    return total, filtered


def get_ingest_status(db: Session) -> Dict[str, Any]:
    def _run():
        cursor_row = (
            db.query(IngestCursor)
            .filter(IngestCursor.cursor_key == "steam_global_catalog")
            .first()
        )
        latest = (
            db.query(IngestJob)
            .filter(IngestJob.job_type == "steam_global_catalog")
            .order_by(desc(IngestJob.created_at))
            .first()
        )
        total_titles = db.query(func.count(SteamTitle.id)).scalar() or 0
        total_assets = db.query(func.count(SteamTitleAsset.id)).scalar() or 0
        total_enrichment = db.query(func.count(SteamDbEnrichment.id)).scalar() or 0
        total_mappings = db.query(func.count(CrossStoreMapping.id)).scalar() or 0
        latest_meta = latest.meta if latest and isinstance(latest.meta, dict) else {}
        latest_external = (
            latest_meta.get("external_enrichment")
            if isinstance(latest_meta.get("external_enrichment"), dict)
            else {}
        )
        return {
            "latest_job": {
                "id": latest.id if latest else None,
                "status": latest.status if latest else "idle",
                "processed_count": latest.processed_count if latest else 0,
                "success_count": latest.success_count if latest else 0,
                "failure_count": latest.failure_count if latest else 0,
                "started_at": latest.started_at.isoformat() if latest and latest.started_at else None,
                "completed_at": latest.completed_at.isoformat() if latest and latest.completed_at else None,
                "error_message": latest.error_message if latest else None,
                "phase": latest_meta.get("phase") if isinstance(latest_meta, dict) else None,
                "cursor": latest_meta.get("cursor") if isinstance(latest_meta, dict) else None,
                "resume_token": latest_meta.get("resume_token") if isinstance(latest_meta, dict) else None,
                "external_enrichment": {
                    "steamdb_success": int(latest_external.get("steamdb_success") or 0),
                    "steamdb_failed": int(latest_external.get("steamdb_failed") or 0),
                    "cross_store_success": int(latest_external.get("cross_store_success") or 0),
                    "cross_store_failed": int(latest_external.get("cross_store_failed") or 0),
                    "completion_processed": int(latest_external.get("completion_processed") or 0),
                    "completion_failed": int(latest_external.get("completion_failed") or 0),
                    "completion_metadata_created": int(
                        latest_external.get("completion_metadata_created") or 0
                    ),
                    "completion_assets_created": int(
                        latest_external.get("completion_assets_created") or 0
                    ),
                    "completion_cross_store_created": int(
                        latest_external.get("completion_cross_store_created") or 0
                    ),
                },
            },
            "cursor": {
                "key": "steam_global_catalog",
                "value": cursor_row.cursor_value if cursor_row else None,
                "meta": cursor_row.cursor_meta if cursor_row else {},
            },
            "totals": {
                "titles": int(total_titles),
                "assets": int(total_assets),
                "steamdb_enrichment": int(total_enrichment),
                "cross_store_mappings": int(total_mappings),
            },
        }

    return _with_schema_retry(_run)
