from __future__ import annotations

import re
import json
import time
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple
from pathlib import Path

import requests
from sqlalchemy import desc, func, or_, select
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
    STEAM_GLOBAL_INDEX_INGEST_BATCH,
    STEAM_GLOBAL_INDEX_MAX_PREFETCH,
    STEAM_GLOBAL_INDEX_SEARCH_LIMIT,
    STEAM_REQUEST_TIMEOUT_SECONDS,
    STEAM_WEB_API_KEY,
    STEAM_WEB_API_URL,
)
from ..db import Base, engine
from ..models import (
    AssetJob,
    CrossStoreMapping,
    IngestJob,
    SteamDbEnrichment,
    SteamTitle,
    SteamTitleAlias,
    SteamTitleAsset,
    SteamTitleMetadata,
)
from .steam_catalog import get_lua_appids, get_steam_detail, get_steam_summary
from .steamgriddb import build_steam_fallback_assets, resolve_assets

_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
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


def ensure_global_index_schema() -> None:
    """
    Ensure new global-index tables exist even on upgraded installs that still
    have an older sqlite file.
    """
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        Base.metadata.create_all(bind=engine)
        _SCHEMA_READY = True


def _with_schema_retry(action):
    ensure_global_index_schema()
    try:
        return action()
    except OperationalError as exc:
        # Handle "no such table" gracefully on stale DBs.
        if "no such table" not in str(exc).lower():
            raise
        ensure_global_index_schema()
        return action()


def normalize_title(value: str) -> str:
    cleaned = _NON_ALNUM.sub(" ", (value or "").strip().lower())
    return " ".join(cleaned.split())


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

    def _pick(types: set[str]) -> Optional[str]:
        for entry in images:
            if not isinstance(entry, dict):
                continue
            image_type = str(entry.get("type") or "").strip().lower()
            url = str(entry.get("url") or "").strip()
            if url and image_type in types:
                return url
        return None

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
    selected["logo"] = _pick({"logo", "offerlogo", "diesellogo"})
    selected["icon"] = _pick({"icon", "square", "thumbnail", "dieselstorefrontsmall"})

    if selected["grid"] is None:
        selected["grid"] = selected["icon"] or selected["hero"]
    if selected["hero"] is None:
        selected["hero"] = selected["grid"] or selected["icon"]
    if selected["icon"] is None:
        selected["icon"] = selected["grid"] or selected["hero"]
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
        "updated_at": datetime.utcnow().isoformat(),
    }
    return confidence


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


def fetch_steam_app_list() -> List[Dict[str, Any]]:
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
    normalized_name = normalize_title(name)
    if row:
        row.name = name
        row.normalized_name = normalized_name
        row.source = source
        row.updated_at = datetime.utcnow()
    else:
        row = SteamTitle(
            app_id=str(app_id),
            name=name,
            normalized_name=normalized_name,
            source=source,
            state="active",
        )
        db.add(row)
        db.flush()
    _upsert_alias(db, row.id, name, locale="en", source=source)
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

    apps = fetch_steam_app_list()
    source = "steam_api"
    if not apps:
        apps = _fallback_apps_from_lua(max_items=max_items)
        source = "lua_fallback"
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

        if enrich_details:
            detail_limit = min(len(appids_for_detail), max_items or len(appids_for_detail))
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
        "started_at": started.isoformat(),
        "completed_at": (job.completed_at or datetime.utcnow()).isoformat(),
    }


def _build_catalog_item(title: SteamTitle) -> Dict[str, Any]:
    metadata = title.metadata_row
    detail_payload = (metadata.detail_payload if metadata else {}) or {}
    summary_payload = (metadata.summary_payload if metadata else {}) or {}
    selected_assets = (title.assets_row.selected_assets if title.assets_row else {}) or {}
    fallback_assets = build_steam_fallback_assets(title.app_id)

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

    return {
        "app_id": title.app_id,
        "name": title.name,
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
        "denuvo": bool((detail_payload or summary_payload).get("denuvo")),
        "artwork": {
            "t0": selected_assets.get("icon") or fallback_assets.get("icon"),
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
        query = db.query(SteamTitle)
        if scope in {"library", "owned"}:
            appids = [str(value) for value in (library_appids or []) if str(value).strip()]
            if not appids:
                return 0, []
            query = query.filter(SteamTitle.app_id.in_(appids))

        sort_value = (sort or "name").lower()
        if sort_value in {"recent", "updated"}:
            query = query.order_by(desc(SteamTitle.updated_at), SteamTitle.name.asc())
        elif sort_value == "appid":
            query = query.order_by(SteamTitle.app_id.asc())
        else:
            query = query.order_by(SteamTitle.name.asc())

        total = query.count()
        rows = query.offset(max(0, offset)).limit(max(1, limit)).all()
        return total, [_build_catalog_item(row) for row in rows]

    return _with_schema_retry(_run)


def search_catalog(
    db: Session,
    q: str,
    limit: int,
    offset: int,
) -> Tuple[int, List[Dict[str, Any]]]:
    def _run():
        query = (q or "").strip()
        if not query:
            return 0, []

        max_limit = min(max(1, limit), max(1, STEAM_GLOBAL_INDEX_SEARCH_LIMIT))
        normalized = normalize_title(query)

        base = db.query(SteamTitle)
        if query.isdigit():
            exact = base.filter(SteamTitle.app_id == query).first()
            if exact:
                return 1, [_build_catalog_item(exact)]

        alias_subq = select(SteamTitleAlias.steam_title_id).where(
            SteamTitleAlias.normalized_alias.ilike(f"%{normalized}%")
        )

        rows_query = (
            base.filter(
                or_(
                    SteamTitle.normalized_name.ilike(f"%{normalized}%"),
                    SteamTitle.app_id.ilike(f"{query}%"),
                    SteamTitle.id.in_(alias_subq),
                )
            )
            .order_by(
                SteamTitle.normalized_name.ilike(f"{normalized}%").desc(),
                SteamTitle.updated_at.desc(),
                SteamTitle.name.asc(),
            )
        )

        total = rows_query.count()
        rows = rows_query.offset(max(0, offset)).limit(max_limit).all()
        return total, [_build_catalog_item(row) for row in rows]

    return _with_schema_retry(_run)


def get_title_detail(db: Session, app_id: str) -> Optional[Dict[str, Any]]:
    def _run():
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
            summary = _build_catalog_item(title)
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

    asset_row = title.assets_row
    if not asset_row:
        asset_row = SteamTitleAsset(steam_title_id=title.id)
        db.add(asset_row)
    asset_row.sgdb_assets = sgdb_assets
    asset_row.epic_assets = epic_assets
    asset_row.steam_assets = steam_assets
    asset_row.selected_assets = selected_assets
    asset_row.selected_source = selected_source
    asset_row.quality_score = score
    asset_row.version = int(asset_row.version or 0) + 1
    asset_row.fetched_at = datetime.utcnow()
    db.commit()
    db.refresh(asset_row)

    return {
        "app_id": app_id,
        "selected_source": selected_source,
        "assets": selected_assets,
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


def get_ingest_status(db: Session) -> Dict[str, Any]:
    def _run():
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
                "external_enrichment": {
                    "steamdb_success": int(latest_external.get("steamdb_success") or 0),
                    "steamdb_failed": int(latest_external.get("steamdb_failed") or 0),
                    "cross_store_success": int(latest_external.get("cross_store_success") or 0),
                    "cross_store_failed": int(latest_external.get("cross_store_failed") or 0),
                },
            },
            "totals": {
                "titles": int(total_titles),
                "assets": int(total_assets),
                "steamdb_enrichment": int(total_enrichment),
                "cross_store_mappings": int(total_mappings),
            },
        }

    return _with_schema_retry(_run)
