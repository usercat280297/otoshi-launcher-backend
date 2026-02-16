import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..core.config import GLOBAL_INDEX_V1
from ..db import get_db
from ..schemas import (
    SearchHistoryIn,
    SearchHistoryOut,
    SteamCatalogOut,
    SteamGameDetailOut,
)
from ..services.settings import add_search_history, clear_search_history, get_search_history
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
    search_catalog as search_global_catalog,
)
from ..services.steam_search import get_popular_catalog, search_catalog
from ..services.steam_extended import (
    get_steam_dlc,
    get_steam_achievements,
    get_steam_player_count,
    get_steam_reviews_summary,
)
from ..services.steam_news_enhanced import fetch_news_enhanced
from ..core.config import STEAM_NEWS_MAX_COUNT
from ..core.cache import cache_client

logger = logging.getLogger(__name__)
router = APIRouter()


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
    art_mode: str = Query("basic", pattern="^(none|basic|tiered)$"),
    thumb_w: int = Query(460, ge=120, le=1024),
    db: Session = Depends(get_db),
):
    appids = get_lua_appids()
    legacy_total = len(appids)

    if GLOBAL_INDEX_V1:
        if search:
            total, items = search_global_catalog(
                db=db,
                q=search,
                limit=limit,
                offset=offset,
            )
            if total > 0 and items:
                items = _inject_artwork(items, art_mode, thumb_w)
                return {
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "items": items,
                }
            # Global index not ready yet. Keep old behavior as fallback.

        else:
            total, items = list_global_catalog(
                db=db,
                limit=limit,
                offset=offset,
                sort=sort,
                scope="all",
            )
            min_ready = max(5000, int(legacy_total * 0.5))
            if total >= min_ready and items:
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
        payload = search_catalog(search, appids, limit, offset, sort)
        items = payload.get("items") or []
        # If Lua-scoped search misses, query global Steam store without Lua filter.
        if not items:
            store_results = search_store(search)
            if store_results:
                candidate_ids = [
                    str(item.get("app_id"))
                    for item in store_results
                    if str(item.get("app_id") or "").strip().isdigit()
                ]
                if candidate_ids:
                    candidate_ids = candidate_ids[offset : offset + limit]
                    items = get_catalog_page(candidate_ids) or store_results[:limit]
                else:
                    items = store_results[:limit]
                payload["total"] = max(int(payload.get("total") or 0), len(store_results))
        items = _inject_artwork(items, art_mode, thumb_w)
        total = int(payload.get("total") or 0)
        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "items": items,
        }

    page_ids = appids[offset : offset + limit]
    items = get_catalog_page(page_ids)
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
            sort="recent",
            scope="all",
        )
        lua_total = len(get_lua_appids())
        min_ready = max(5000, int(lua_total * 0.5))
        if total >= min_ready and items:
            return {
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": items,
            }

    payload = get_popular_catalog(limit, offset)
    return {
        "total": int(payload.get("total") or 0),
        "offset": offset,
        "limit": limit,
        "items": payload.get("items") or [],
    }


@router.get("/games/{app_id}", response_model=SteamGameDetailOut)
def game_detail(app_id: str, db: Session = Depends(get_db)):
    if GLOBAL_INDEX_V1:
        detail = get_global_index_title_detail(db, app_id)
        if detail:
            return detail
        # Global index can be empty before first ingest; fall through to legacy path.

    detail = get_steam_detail(app_id)
    if not detail:
        detail = _build_fallback_detail_from_summary(app_id)
    if not detail:
        detail = _build_fallback_detail_from_download_options(app_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Steam app not found")
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
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
    from ..core.cache import cache_client
    import functools
    
    # Check if we have cached the entire response
    cache_key = f"steam:game:extended:v3:{app_id}:{news_count}:{int(news_all)}"
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
                dlc_result = dlc_future.result(timeout=10)
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
