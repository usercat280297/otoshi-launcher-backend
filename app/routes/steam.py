import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query

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

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/catalog", response_model=SteamCatalogOut)
def catalog(
    limit: int = Query(24, ge=1, le=100),
    offset: int = Query(0, ge=0),
    search: str | None = Query(None),
    sort: str | None = Query(None),
):
    appids = get_lua_appids()
    total = len(appids)

    if search:
        payload = search_catalog(search, appids, limit, offset, sort)
        items = payload.get("items") or []
        total = int(payload.get("total") or 0)
        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "items": items,
        }

    page_ids = appids[offset : offset + limit]
    items = get_catalog_page(page_ids)
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
def popular(limit: int = Query(12, ge=1, le=100), offset: int = Query(0, ge=0)):
    payload = get_popular_catalog(limit, offset)
    return {
        "total": int(payload.get("total") or 0),
        "offset": offset,
        "limit": limit,
        "items": payload.get("items") or [],
    }


@router.get("/games/{app_id}", response_model=SteamGameDetailOut)
def game_detail(app_id: str):
    detail = get_steam_detail(app_id)
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
