from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import (
    SteamCatalogOut,
    SteamGameDetailOut,
    SteamIndexAssetOut,
    SteamIndexAssetBatchOut,
    SteamIndexClassificationOut,
    SteamIndexCoverageOut,
    SteamIndexCompletionIn,
    SteamIndexCompletionOut,
    SteamIndexAssetPrefetchIn,
    SteamIndexAssetPrefetchOut,
    SteamIndexIngestFullIn,
    SteamIndexIngestRebuildIn,
    SteamIndexIngestRebuildOut,
    SteamIndexIngestResumeIn,
    SteamIndexIngestStatusOut,
    SteamIndexRankingOut,
)
from ..services.settings import detect_system_locale, get_user_locale, normalize_locale
from ..services.steam_catalog import get_catalog_page, get_lua_appids, get_steam_detail, search_store
from ..services.steam_extended import (
    get_steam_achievements,
    get_steam_dlc,
    get_steam_player_count,
    get_steam_reviews_summary,
)
from ..services.steam_global_index import (
    get_catalog_coverage,
    get_ingest_status,
    get_title_classification,
    get_title_detail,
    enforce_catalog_completeness,
    ingest_full_catalog,
    ingest_global_catalog,
    list_catalog,
    list_top_ranked,
    prefetch_assets,
    prefetch_assets_force_visible,
    resume_ingest_catalog,
    resolve_assets_chain,
    search_catalog,
)
from ..services.steamgriddb import build_steam_fallback_assets
from ..services.steam_news_enhanced import fetch_news_enhanced
from .deps import require_admin_access

router = APIRouter()

_LOCALIZED_DETAIL_FIELDS = (
    "name",
    "short_description",
    "about_the_game",
    "about_the_game_html",
    "detailed_description",
    "detailed_description_html",
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
    include_dlc: bool | None = Query(None),
    ranking_mode: str | None = Query(None, pattern="^(relevance|recent|updated|priority|hot|top)?$"),
    must_have_artwork: bool = Query(False),
    db: Session = Depends(get_db),
):
    total, items = search_catalog(
        db=db,
        q=q,
        limit=limit,
        offset=offset,
        include_dlc=include_dlc,
        ranking_mode=ranking_mode,
        must_have_artwork=must_have_artwork,
    )
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
def steam_index_game_detail(
    app_id: str,
    locale: str | None = Query(None),
    db: Session = Depends(get_db),
):
    resolved_locale = _resolve_content_locale(locale)
    detail = get_title_detail(db, app_id)
    localized = get_steam_detail(app_id, locale=resolved_locale)
    if not detail:
        if localized:
            return _merge_localized_detail(localized, localized, resolved_locale)
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
            "content_locale": resolved_locale,
        }
    return _merge_localized_detail(detail, localized, resolved_locale)


@router.get("/games/{app_id}/classification", response_model=SteamIndexClassificationOut)
def steam_index_game_classification(
    app_id: str,
    db: Session = Depends(get_db),
):
    return get_title_classification(db, app_id)


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


@router.post("/assets/prefetch-force-visible", response_model=SteamIndexAssetPrefetchOut)
def steam_index_assets_prefetch_force_visible(
    payload: SteamIndexAssetPrefetchIn,
    db: Session = Depends(get_db),
):
    return prefetch_assets_force_visible(
        db,
        app_ids=payload.app_ids,
    )


@router.post("/assets/batch", response_model=SteamIndexAssetBatchOut)
def steam_index_assets_batch(
    payload: SteamIndexAssetPrefetchIn,
    db: Session = Depends(get_db),
):
    items = {}
    for raw_id in payload.app_ids:
        app_id = str(raw_id or "").strip()
        if not app_id.isdigit():
            continue
        try:
            resolved = resolve_assets_chain(db, app_id, force_refresh=payload.force_refresh)
        except Exception:
            resolved = {
                "app_id": app_id,
                "selected_source": "steam",
                "assets": build_steam_fallback_assets(app_id),
                "quality_score": 0.0,
                "version": 1,
            }
        items[app_id] = resolved
    return {"items": items}


@router.get("/ingest/status", response_model=SteamIndexIngestStatusOut)
def steam_index_ingest_status(db: Session = Depends(get_db)):
    return get_ingest_status(db)


@router.get("/coverage", response_model=SteamIndexCoverageOut)
def steam_index_coverage(db: Session = Depends(get_db)):
    return get_catalog_coverage(db)


@router.get("/ranking/top", response_model=SteamIndexRankingOut)
def steam_index_ranking_top(
    limit: int = Query(12, ge=1, le=100),
    offset: int = Query(0, ge=0),
    include_dlc: bool = Query(False),
    db: Session = Depends(get_db),
):
    total, items = list_top_ranked(
        db,
        limit=limit,
        offset=offset,
        include_dlc=include_dlc,
    )
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }


@router.post("/ingest/full", response_model=SteamIndexIngestRebuildOut)
def steam_index_ingest_full(
    payload: SteamIndexIngestFullIn,
    _: object = Depends(require_admin_access),
    db: Session = Depends(get_db),
):
    return ingest_full_catalog(
        db=db,
        max_items=payload.max_items,
    )


@router.post("/ingest/resume", response_model=SteamIndexIngestRebuildOut)
def steam_index_ingest_resume(
    payload: SteamIndexIngestResumeIn,
    _: object = Depends(require_admin_access),
    db: Session = Depends(get_db),
):
    return resume_ingest_catalog(
        db=db,
        resume_token=payload.resume_token,
        max_items=payload.max_items,
    )


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


@router.post("/ingest/complete", response_model=SteamIndexCompletionOut)
def steam_index_ingest_complete(
    payload: SteamIndexCompletionIn,
    _: object = Depends(require_admin_access),
    db: Session = Depends(get_db),
):
    return enforce_catalog_completeness(
        db=db,
        app_ids=payload.app_ids,
        max_items=payload.max_items,
    )
