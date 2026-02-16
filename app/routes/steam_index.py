from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import (
    SteamCatalogOut,
    SteamGameDetailOut,
    SteamIndexAssetOut,
    SteamIndexAssetPrefetchIn,
    SteamIndexAssetPrefetchOut,
    SteamIndexIngestRebuildIn,
    SteamIndexIngestRebuildOut,
    SteamIndexIngestStatusOut,
)
from ..services.steam_catalog import get_catalog_page, get_lua_appids, search_store
from ..services.steam_extended import (
    get_steam_achievements,
    get_steam_dlc,
    get_steam_player_count,
    get_steam_reviews_summary,
)
from ..services.steam_global_index import (
    get_ingest_status,
    get_title_detail,
    ingest_global_catalog,
    list_catalog,
    prefetch_assets,
    resolve_assets_chain,
    search_catalog,
)
from ..services.steam_news_enhanced import fetch_news_enhanced
from .deps import require_admin_access

router = APIRouter()


@router.get("/catalog", response_model=SteamCatalogOut)
def steam_index_catalog(
    limit: int = Query(24, ge=1, le=200),
    offset: int = Query(0, ge=0),
    sort: str | None = Query(None),
    scope: str = Query("all", pattern="^(all|library|owned)$"),
    db: Session = Depends(get_db),
):
    library_appids = get_lua_appids() if scope in {"library", "owned"} else None
    total, items = list_catalog(
        db=db,
        limit=limit,
        offset=offset,
        sort=sort,
        scope=scope,
        library_appids=library_appids,
    )
    if total <= 0:
        # Fallback before first ingest: keep endpoint useful by serving legacy
        # catalog data from Lua-backed appids.
        fallback_ids = get_lua_appids()
        if scope in {"library", "owned"}:
            fallback_ids = library_appids or []
        page_ids = fallback_ids[offset : offset + limit]
        items = get_catalog_page(page_ids) if page_ids else []
        total = len(fallback_ids)
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }


@router.get("/search", response_model=SteamCatalogOut)
def steam_index_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(24, ge=1, le=200),
    offset: int = Query(0, ge=0),
    source: str = Query("global", pattern="^(global)$"),
    db: Session = Depends(get_db),
):
    total, items = search_catalog(db=db, q=q, limit=limit, offset=offset)
    if total <= 0 or not items:
        # Fallback path before first ingest: search upstream Steam store directly.
        store_results = search_store(q)
        candidate_ids = [
            str(item.get("app_id"))
            for item in store_results
            if str(item.get("app_id") or "").strip().isdigit()
        ]
        if candidate_ids:
            page_ids = candidate_ids[offset : offset + limit]
            items = get_catalog_page(page_ids)
            total = len(candidate_ids)
        else:
            total = 0
            items = []
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }


@router.get("/games/{app_id}", response_model=SteamGameDetailOut)
def steam_index_game_detail(app_id: str, db: Session = Depends(get_db)):
    detail = get_title_detail(db, app_id)
    if not detail:
        return {
            "app_id": app_id,
            "name": f"Steam App {app_id}",
            "short_description": None,
            "header_image": None,
            "capsule_image": None,
            "background": None,
            "about_the_game": None,
            "about_the_game_html": None,
            "detailed_description": None,
            "detailed_description_html": None,
            "developers": [],
            "publishers": [],
            "categories": [],
            "screenshots": [],
            "movies": [],
            "pc_requirements": {},
            "metacritic": None,
            "recommendations": None,
            "website": None,
            "support_info": None,
            "genres": [],
            "platforms": [],
            "denuvo": False,
        }
    return detail


@router.get("/games/{app_id}/extended")
def steam_index_game_extended(
    app_id: str,
    news_count: int = Query(5, ge=1, le=200),
    news_all: bool = Query(False),
):
    resolved_news_count = 0 if news_all else news_count
    dlc = get_steam_dlc(app_id) or []
    achievements = get_steam_achievements(app_id) or []
    news = fetch_news_enhanced(app_id, resolved_news_count) or []
    player_count = get_steam_player_count(app_id)
    reviews = get_steam_reviews_summary(app_id) or {
        "total_positive": 0,
        "total_negative": 0,
        "total_reviews": 0,
        "review_score": 0,
        "review_score_desc": "No reviews",
    }
    return {
        "app_id": app_id,
        "dlc": {"items": dlc, "total": len(dlc)},
        "achievements": {"items": achievements, "total": len(achievements)},
        "news": {"items": news, "total": len(news)},
        "player_count": player_count,
        "reviews": reviews,
    }


@router.get("/assets/{app_id}", response_model=SteamIndexAssetOut)
def steam_index_assets(
    app_id: str,
    force_refresh: bool = Query(False),
    db: Session = Depends(get_db),
):
    payload = resolve_assets_chain(db, app_id, force_refresh=force_refresh)
    return payload


@router.post("/assets/prefetch", response_model=SteamIndexAssetPrefetchOut)
def steam_index_assets_prefetch(
    payload: SteamIndexAssetPrefetchIn,
    db: Session = Depends(get_db),
):
    result = prefetch_assets(
        db,
        app_ids=payload.app_ids,
        force_refresh=payload.force_refresh,
    )
    return result


@router.get("/ingest/status", response_model=SteamIndexIngestStatusOut)
def steam_index_ingest_status(db: Session = Depends(get_db)):
    return get_ingest_status(db)


@router.post("/ingest/rebuild", response_model=SteamIndexIngestRebuildOut)
def steam_index_ingest_rebuild(
    payload: SteamIndexIngestRebuildIn,
    _: object = Depends(require_admin_access),
    db: Session = Depends(get_db),
):
    return ingest_global_catalog(
        db=db,
        max_items=payload.max_items,
        enrich_details=payload.enrich_details,
    )
