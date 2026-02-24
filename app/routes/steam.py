import logging
from contextlib import nullcontext
from threading import Lock
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..core.config import (
    AI_FEATURE_SEARCH_HYBRID,
    AI_SEARCH_DEFAULT_MODE,
    GLOBAL_INDEX_V1,
    STEAM_CATALOG_CACHE_TTL_SECONDS,
)
from ..db import get_db
from ..models import User
from ..schemas import (
    SearchHistoryIn,
    SearchHistoryOut,
    SteamCatalogOut,
    SteamGameDetailOut,
)
from ..services.ai_search import search_catalog_ai
from ..services.settings import (
    add_search_history,
    clear_search_history,
    detect_system_locale,
    get_search_history,
    get_user_locale,
    normalize_locale,
)
from ..services.steam_catalog import (
    get_catalog_page,
    get_lua_appids,
    get_steam_detail,
    get_steam_summary,
    search_store,
)
from ..services.download_options import build_download_options
from ..services.steam_global_index import (
    get_title_detail as get_global_index_title_detail,
    list_catalog as list_global_catalog,
)
from ..services.steam_search import get_popular_catalog, normalize_text, search_catalog
from ..services.steam_extended import (
    get_steam_dlc,
    get_steam_achievements,
    get_steam_player_count,
    get_steam_reviews_summary,
)
from ..services.steam_news_enhanced import fetch_news_enhanced
from ..core.config import STEAM_NEWS_MAX_COUNT
from ..core.cache import cache_client
from .deps import get_current_user_optional

logger = logging.getLogger(__name__)
router = APIRouter()

_LOCALIZED_DETAIL_FIELDS = (
    "name",
    "short_description",
    "header_image",
    "capsule_image",
    "background",
    "about_the_game",
    "about_the_game_html",
    "detailed_description",
    "detailed_description_html",
    "screenshots",
    "movies",
    "pc_requirements",
    "metacritic",
    "recommendations",
    "website",
    "support_info",
    "price",
    "genres",
    "categories",
    "developers",
    "publishers",
    "release_date",
    "platforms",
    "required_age",
    "item_type",
    "is_dlc",
    "dlc_count",
)

_CATALOG_SEARCH_ROUTE_CACHE_VERSION = 1
_CATALOG_SEARCH_CACHE_WINDOW = 120
_CATALOG_SEARCH_MAX_WINDOW = 240
_CATALOG_PRICE_BACKFILL_MAX_FETCH = 6
_CATALOG_ROUTE_LOCK_GUARD = Lock()
_CATALOG_ROUTE_LOCKS: Dict[str, Lock] = {}


def _resolve_catalog_search_window(limit: int, offset: int) -> int:
    target = max(_CATALOG_SEARCH_CACHE_WINDOW, int(limit) + int(offset))
    return max(1, min(_CATALOG_SEARCH_MAX_WINDOW, target))


def _resolve_price_backfill_fetch(limit: int) -> int:
    return max(0, min(_CATALOG_PRICE_BACKFILL_MAX_FETCH, int(limit)))


def _build_catalog_search_route_cache_key(
    query: str,
    mode: str,
    sort: str | None,
    appids: List[str],
) -> str:
    normalized_query = normalize_text(query) or query.strip().lower()
    sort_key = (sort or "relevance").strip().lower() or "relevance"
    if appids:
        appids_sig = f"{len(appids)}:{appids[0]}:{appids[-1]}"
    else:
        appids_sig = "0"
    return (
        f"steam:catalog:search:v{_CATALOG_SEARCH_ROUTE_CACHE_VERSION}:"
        f"{normalized_query}:{mode}:{sort_key}:{appids_sig}"
    )


def _get_catalog_route_lock(key: str) -> Lock:
    with _CATALOG_ROUTE_LOCK_GUARD:
        lock = _CATALOG_ROUTE_LOCKS.get(key)
        if lock is None:
            lock = Lock()
            _CATALOG_ROUTE_LOCKS[key] = lock
        return lock


def _resolve_content_locale(preferred: str | None) -> str:
    if preferred:
        return normalize_locale(preferred)
    user_locale = get_user_locale()
    if user_locale:
        return normalize_locale(user_locale)
    return normalize_locale(detect_system_locale())


def _merge_localized_detail(base: dict, localized: dict | None, resolved_locale: str) -> dict:
    if not isinstance(base, dict):
        return base
    if not isinstance(localized, dict):
        base["content_locale"] = resolved_locale
        return base
    merged = dict(base)
    for field in _LOCALIZED_DETAIL_FIELDS:
        value = localized.get(field)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, list) and not value:
            continue
        merged[field] = value
    merged["content_locale"] = localized.get("content_locale") or resolved_locale
    return merged


def _clamp_thumb_width(value: int) -> int:
    return max(120, min(1024, int(value)))


def _build_artwork_tiers(
    item: Dict[str, Any],
    mode: str,
    thumb_w: int,
) -> Dict[str, Any] | None:
    if mode == "none":
        return None

    header = item.get("header_image")
    capsule = item.get("capsule_image")
    background = item.get("background") or capsule or header
    if not any([header, capsule, background]):
        return None

    _ = _clamp_thumb_width(thumb_w)
    if mode == "basic":
        return {
            "t2": capsule or header,
            "t3": background or header,
            "version": 1,
        }

    return {
        "t0": capsule or header,
        "t1": capsule or header,
        "t2": capsule or header,
        "t3": background or header,
        "t4": header or background,
        "version": 1,
    }


def _inject_artwork(
    items: List[Dict[str, Any]],
    mode: str,
    thumb_w: int,
) -> List[Dict[str, Any]]:
    if mode == "none":
        return items

    enriched: List[Dict[str, Any]] = []
    for item in items:
        clone = dict(item)
        artwork = _build_artwork_tiers(clone, mode, thumb_w)
        if artwork:
            clone["artwork"] = artwork
        enriched.append(clone)
    return enriched


def _normalize_price_payload(price: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(price, dict):
        return None
    normalized = {
        "initial": price.get("initial"),
        "final": price.get("final"),
        "discount_percent": price.get("discount_percent"),
        "currency": price.get("currency"),
        "formatted": price.get("formatted"),
        "final_formatted": price.get("final_formatted"),
    }
    if all(value is None for value in normalized.values()):
        return None
    if normalized.get("final") == 0 and not normalized.get("final_formatted"):
        normalized["final_formatted"] = "Free"
    if normalized.get("initial") == 0 and not normalized.get("formatted"):
        normalized["formatted"] = "Free"
    return normalized


def _backfill_missing_prices(
    items: List[Dict[str, Any]],
    *,
    max_fetch: int = 24,
) -> List[Dict[str, Any]]:
    if not items:
        return items

    fetch_limit = max(0, int(max_fetch))
    candidate_ids: List[str] = []
    seen: set[str] = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        if _normalize_price_payload(item.get("price")):
            continue
        app_id = str(item.get("app_id") or "").strip()
        if not app_id.isdigit() or app_id in seen:
            continue
        seen.add(app_id)
        candidate_ids.append(app_id)
        if len(candidate_ids) >= fetch_limit:
            break

    price_map: Dict[str, Dict[str, Any]] = {}
    if candidate_ids:
        summaries = get_catalog_page(candidate_ids) or []
        for summary in summaries:
            if not isinstance(summary, dict):
                continue
            app_id = str(summary.get("app_id") or "").strip()
            price = _normalize_price_payload(summary.get("price"))
            if app_id and price:
                price_map[app_id] = price

    patched: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            patched.append(item)
            continue
        clone = dict(item)
        price = _normalize_price_payload(clone.get("price"))
        if not price:
            app_id = str(clone.get("app_id") or "").strip()
            if app_id in price_map:
                price = price_map[app_id]
        if price:
            clone["price"] = price
        patched.append(clone)
    return patched


def _build_fallback_detail_from_summary(app_id: str) -> Optional[dict]:
    summary = get_steam_summary(app_id)
    if not summary:
        page = get_catalog_page([app_id])
        if page:
            summary = page[0]
    if not summary:
        return None

    header_image = summary.get("header_image")
    capsule_image = summary.get("capsule_image")
    background = summary.get("background") or capsule_image or header_image

    return {
        "app_id": str(summary.get("app_id") or app_id),
        "name": summary.get("name") or f"Steam App {app_id}",
        "short_description": summary.get("short_description"),
        "header_image": header_image,
        "capsule_image": capsule_image,
        "background": background,
        "required_age": summary.get("required_age"),
        "price": summary.get("price"),
        "genres": summary.get("genres"),
        "release_date": summary.get("release_date"),
        "platforms": summary.get("platforms"),
        "denuvo": summary.get("denuvo"),
        "about_the_game": summary.get("short_description"),
        "about_the_game_html": None,
        "detailed_description": summary.get("short_description"),
        "detailed_description_html": None,
        "developers": None,
        "publishers": None,
        "categories": None,
        "screenshots": [value for value in [header_image, background] if value],
        "movies": [],
        "pc_requirements": None,
        "metacritic": None,
        "recommendations": None,
        "website": None,
        "support_info": None,
    }


def _build_fallback_detail_from_download_options(app_id: str) -> Optional[dict]:
    options = build_download_options(app_id)
    if not options:
        return None

    name = options.get("name") or f"Steam App {app_id}"
    size_label = options.get("size_label")
    short_description = (
        f"Chunk manifest available ({size_label})."
        if size_label
        else "Chunk manifest available."
    )

    return {
        "app_id": str(app_id),
        "name": name,
        "short_description": short_description,
        "header_image": None,
        "capsule_image": None,
        "background": None,
        "required_age": None,
        "price": None,
        "genres": [],
        "release_date": None,
        "platforms": ["windows"],
        "denuvo": False,
        "about_the_game": short_description,
        "about_the_game_html": None,
        "detailed_description": short_description,
        "detailed_description_html": None,
        "developers": None,
        "publishers": None,
        "categories": None,
        "screenshots": [],
        "movies": [],
        "pc_requirements": None,
        "metacritic": None,
        "recommendations": None,
        "website": None,
        "support_info": None,
    }


@router.get("/catalog", response_model=SteamCatalogOut)
def catalog(
    limit: int = Query(24, ge=1, le=100),
    offset: int = Query(0, ge=0),
    search: str | None = Query(None),
    sort: str | None = Query(None),
    search_mode: str | None = Query(None, pattern="^(lexical|hybrid|semantic)$"),
    explain: bool = Query(False),
    art_mode: str = Query("basic", pattern="^(none|basic|tiered)$"),
    thumb_w: int = Query(460, ge=120, le=1024),
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional),
):
    appids = get_lua_appids()
    legacy_total = len(appids)

    if GLOBAL_INDEX_V1 and not search:
        try:
            total, items = list_global_catalog(
                db=db,
                limit=limit,
                offset=offset,
                sort=sort,
                scope="all",
            )
        except Exception:
            logger.exception("Global catalog list failed; fallback to legacy steam catalog")
            total, items = 0, []
        min_ready = max(5000, int(legacy_total * 0.5))
        if total >= min_ready and items:
            items = _backfill_missing_prices(items, max_fetch=_resolve_price_backfill_fetch(limit))
            items = _inject_artwork(items, art_mode, thumb_w)
            return {
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": items,
            }
        # Global index not ready yet. Keep old behavior as fallback.

    total = legacy_total

    if search:
        resolved_mode = (search_mode or AI_SEARCH_DEFAULT_MODE or "lexical").strip().lower()
        if resolved_mode not in {"lexical", "hybrid", "semantic"}:
            resolved_mode = "lexical"
        if resolved_mode in {"hybrid", "semantic"} and not AI_FEATURE_SEARCH_HYBRID and not search_mode:
            resolved_mode = "lexical"
        # Anonymous traffic uses lexical path to keep p95 stable on hot endpoint.
        effective_mode = (
            "lexical"
            if current_user is None and not explain and resolved_mode in {"hybrid", "semantic"}
            else resolved_mode
        )
        enable_route_cache = current_user is None and not explain
        route_cache_key: str | None = None
        if enable_route_cache:
            route_cache_key = _build_catalog_search_route_cache_key(search, effective_mode, sort, appids)
        route_lock = _get_catalog_route_lock(route_cache_key) if route_cache_key else None

        with (route_lock or nullcontext()):
            if route_cache_key:
                cached_payload = cache_client.get_json(route_cache_key)
                if isinstance(cached_payload, dict):
                    cached_items = cached_payload.get("items")
                    if isinstance(cached_items, list):
                        paged_items = cached_items[offset : offset + limit]
                        paged_items = _inject_artwork(paged_items, art_mode, thumb_w)
                        return {
                            "total": int(cached_payload.get("total") or len(cached_items)),
                            "offset": offset,
                            "limit": limit,
                            "items": paged_items,
                        }

            search_limit = _resolve_catalog_search_window(limit, offset) if enable_route_cache else limit
            search_offset = 0 if enable_route_cache else offset
            if effective_mode == "lexical":
                payload = search_catalog(search, appids, search_limit, search_offset, sort)
            else:
                payload = search_catalog_ai(
                    db=db,
                    query=search,
                    allowed_appids=appids,
                    limit=search_limit,
                    offset=search_offset,
                    sort=sort,
                    mode=effective_mode,
                    user_id=current_user.id if current_user else None,
                    explain=explain,
                )
            items = payload.get("items") or []
            # If Lua-scoped lexical search misses, query global Steam store without Lua filter.
            if not items and effective_mode == "lexical" and search_offset == 0:
                store_results = search_store(search)
                if store_results:
                    candidate_ids = [
                        str(item.get("app_id"))
                        for item in store_results
                        if str(item.get("app_id") or "").strip().isdigit()
                    ]
                    if candidate_ids:
                        candidate_ids = candidate_ids[:search_limit]
                        items = get_catalog_page(candidate_ids) or store_results[:search_limit]
                    else:
                        items = store_results[:search_limit]
                    payload["total"] = max(int(payload.get("total") or 0), len(store_results))

            items = _backfill_missing_prices(items, max_fetch=_resolve_price_backfill_fetch(search_limit))
            total = int(payload.get("total") or len(items))
            if route_cache_key:
                cache_client.set_json(
                    route_cache_key,
                    {"total": total, "items": items},
                    ttl=STEAM_CATALOG_CACHE_TTL_SECONDS,
                )
                items = items[offset : offset + limit]
            items = _inject_artwork(items, art_mode, thumb_w)
            return {
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": items,
            }

    page_ids = appids[offset : offset + limit]
    items = get_catalog_page(page_ids)
    items = _backfill_missing_prices(items, max_fetch=_resolve_price_backfill_fetch(limit))
    items = _inject_artwork(items, art_mode, thumb_w)
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }


@router.get("/search/history", response_model=SearchHistoryOut)
def search_history(limit: int = Query(12, ge=1, le=50)):
    return {"items": get_search_history(limit)}


@router.post("/search/history", response_model=SearchHistoryOut)
def record_search_history(payload: SearchHistoryIn, limit: int = Query(12, ge=1, le=50)):
    add_search_history(payload.query, limit)
    return {"items": get_search_history(limit)}


@router.delete("/search/history", response_model=SearchHistoryOut)
def remove_search_history():
    clear_search_history()
    return {"items": []}


@router.get("/search/popular", response_model=SteamCatalogOut)
def popular(
    limit: int = Query(12, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    if GLOBAL_INDEX_V1:
        total, items = list_global_catalog(
            db=db,
            limit=limit,
            offset=offset,
            sort="priority",
            scope="all",
        )
        lua_total = len(get_lua_appids())
        min_ready = max(5000, int(lua_total * 0.5))
        if total >= min_ready and items:
            items = _backfill_missing_prices(items, max_fetch=_resolve_price_backfill_fetch(limit))
            return {
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": items,
            }

    payload = get_popular_catalog(limit, offset)
    items = _backfill_missing_prices(
        payload.get("items") or [],
        max_fetch=_resolve_price_backfill_fetch(limit),
    )
    return {
        "total": int(payload.get("total") or 0),
        "offset": offset,
        "limit": limit,
        "items": items,
    }


@router.get("/games/{app_id}", response_model=SteamGameDetailOut)
def game_detail(
    app_id: str,
    locale: str | None = Query(None),
    db: Session = Depends(get_db),
):
    resolved_locale = _resolve_content_locale(locale)
    if GLOBAL_INDEX_V1:
        detail = get_global_index_title_detail(db, app_id)
        if detail:
            localized = get_steam_detail(app_id, locale=resolved_locale)
            return _merge_localized_detail(detail, localized, resolved_locale)
        # Global index can be empty before first ingest; fall through to legacy path.

    detail = get_steam_detail(app_id, locale=resolved_locale)
    if not detail:
        detail = _build_fallback_detail_from_summary(app_id)
    if not detail:
        detail = _build_fallback_detail_from_download_options(app_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Steam app not found")
    if isinstance(detail, dict):
        detail["content_locale"] = detail.get("content_locale") or resolved_locale
    return detail


@router.get("/games/{app_id}/dlc")
def game_dlc(app_id: str):
    """Get DLC list for a Steam game"""
    dlc_list = get_steam_dlc(app_id)
    return {"app_id": app_id, "items": dlc_list, "total": len(dlc_list)}


@router.get("/games/{app_id}/achievements")
def game_achievements(app_id: str):
    """Get achievements for a Steam game"""
    achievements = get_steam_achievements(app_id)
    return {"app_id": app_id, "items": achievements, "total": len(achievements)}


@router.get("/games/{app_id}/news")
def game_news(
    app_id: str,
    count: int = Query(10, ge=1, le=STEAM_NEWS_MAX_COUNT),
    all: bool = Query(False),
):
    """Get news/updates for a Steam game with enhanced formatting"""
    news = fetch_news_enhanced(app_id, 0 if all else count)
    return {"app_id": app_id, "items": news, "total": len(news)}


@router.get("/games/{app_id}/players")
def game_players(app_id: str):
    """Get current player count for a Steam game"""
    count = get_steam_player_count(app_id)
    return {"app_id": app_id, "player_count": count}


@router.get("/games/{app_id}/reviews")
def game_reviews(app_id: str):
    """Get review summary for a Steam game"""
    summary = get_steam_reviews_summary(app_id)
    if not summary:
        return {
            "app_id": app_id,
            "total_positive": 0,
            "total_negative": 0,
            "total_reviews": 0,
            "review_score": 0,
            "review_score_desc": "No reviews",
        }
    return {"app_id": app_id, **summary}


@router.get("/games/{app_id}/extended")
def game_extended(
    app_id: str,
    news_count: int = Query(5, ge=1, le=STEAM_NEWS_MAX_COUNT),
    news_all: bool = Query(False),
):
    """Get all extended data for a Steam game (DLC, achievements, news, players, reviews)"""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from ..core.cache import cache_client
    
    DLC_TIMEOUT_SECONDS = 25
    
    # Check if we have cached the entire response
    cache_key = f"steam:game:extended:v5:{app_id}:{news_count}:{int(news_all)}"
    cached_response = cache_client.get_json(cache_key)
    if cached_response is not None:
        return cached_response
    
    # Wrap functions with timeouts
    def safe_call(func, *args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            return None
    
    # Use thread pool to fetch data in parallel
    dlc_result = None
    achievements_result = None
    news_result = None
    player_count_result = None
    reviews_result = None
    
    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all tasks
            dlc_future = executor.submit(safe_call, get_steam_dlc, app_id)
            ach_future = executor.submit(safe_call, get_steam_achievements, app_id)
            resolved_news_count = 0 if news_all else news_count
            news_future = executor.submit(safe_call, fetch_news_enhanced, app_id, resolved_news_count)
            player_future = executor.submit(safe_call, get_steam_player_count, app_id)
            review_future = executor.submit(safe_call, get_steam_reviews_summary, app_id)
            
            # Get results with timeout
            try:
                dlc_result = dlc_future.result(timeout=DLC_TIMEOUT_SECONDS)
            except TimeoutError:
                logger.warning(f"DLC fetch timeout for {app_id}")
                
            try:
                achievements_result = ach_future.result(timeout=10)
            except TimeoutError:
                logger.warning(f"Achievements fetch timeout for {app_id}")
                
            try:
                news_result = news_future.result(timeout=10)
            except TimeoutError:
                logger.warning(f"News fetch timeout for {app_id}")
                
            try:
                player_count_result = player_future.result(timeout=5)
            except TimeoutError:
                logger.warning(f"Player count fetch timeout for {app_id}")
                
            try:
                reviews_result = review_future.result(timeout=10)
            except TimeoutError:
                logger.warning(f"Reviews fetch timeout for {app_id}")
                
    except Exception as e:
        logger.error(f"ThreadPoolExecutor error: {e}")

    # Build response with fallbacks
    dlc = dlc_result or []
    achievements = achievements_result or []
    news = news_result or []
    player_count = player_count_result
    reviews = reviews_result or {
        "total_positive": 0,
        "total_negative": 0,
        "total_reviews": 0,
        "review_score": 0,
        "review_score_desc": "No reviews",
    }

    response = {
        "app_id": app_id,
        "dlc": {"items": dlc, "total": len(dlc)},
        "achievements": {"items": achievements, "total": len(achievements)},
        "news": {"items": news, "total": len(news)},
        "player_count": player_count,
        "reviews": reviews,
    }
    
    # Cache the entire response for 5 minutes
    cache_client.set_json(cache_key, response, ttl=300)
    
    return response


@router.post("/games/{app_id}/cache/clear")
def clear_game_cache(app_id: str):
    prefixes = [
        f"steam:summary:{app_id}",
        f"steam:detail:{app_id}",
        f"steam:dlc:{app_id}",
        f"steam:achievements:{app_id}",
        f"steam:news:v7:{app_id}:",
        f"steam:game:extended:v3:{app_id}:",
        f"steam:game:extended:v4:{app_id}:",
        f"steam:game:extended:v5:{app_id}:",
    ]
    exact_keys = [
        f"steam:players:{app_id}",
        f"steam:reviews:{app_id}",
    ]

    cleared = 0
    for key in exact_keys:
        cache_client.delete(key)
        cleared += 1
    for prefix in prefixes:
        cleared += cache_client.delete_prefix(prefix)

    return {"app_id": app_id, "cleared": cleared}
