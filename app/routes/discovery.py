from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..core.cache import cache_client
from ..db import get_db
from ..models import Game, User
from ..schemas import (
    AnimeDetailOut,
    AnimeEpisodeSourceOut,
    AnimeHomeOut,
    AnimeItemOut,
    GameOut,
    SteamCatalogOut,
)
from ..core.config import AI_FEATURE_RECO_V2, DISCOVERY_FORCE_STEAM
from ..services.ai_recommendations import recommend_v2
from ..services.recommendations import recommend_games, similar_games
from ..services.steam_catalog import get_catalog_page, get_lua_appids
from ..services.anime_catalog import (
    get_anime_detail,
    get_episode_sources,
    get_home_sections,
    search_home_catalog,
)
from .deps import get_current_user

router = APIRouter()


@router.get("/queue", response_model=List[GameOut])
def discovery_queue(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    cache_key = f"discovery:queue:{current_user.id}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached
    queue = recommend_games(db, current_user.id, limit=12)
    payload = [GameOut.model_validate(game).model_dump() for game in queue]

    if DISCOVERY_FORCE_STEAM or len(payload) == 0:
        appids = get_lua_appids()
        steam_items = get_catalog_page(appids[:12]) if appids else []
        payload = [_steam_summary_to_game(item) for item in steam_items]

    cache_client.set_json(cache_key, payload, ttl=300)
    return payload


@router.post("/queue/refresh", response_model=List[GameOut])
def refresh_discovery_queue(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    cache_client.delete(f"discovery:queue:{current_user.id}")
    queue = recommend_games(db, current_user.id, limit=12)
    payload = [GameOut.model_validate(game).model_dump() for game in queue]
    if DISCOVERY_FORCE_STEAM or len(payload) == 0:
        appids = get_lua_appids()
        steam_items = get_catalog_page(appids[:12]) if appids else []
        payload = [_steam_summary_to_game(item) for item in steam_items]
    return payload


@router.get("/anime/home", response_model=AnimeHomeOut)
def anime_home(
    limit_per_section: int = Query(12, ge=1, le=24),
    refresh: bool = Query(False),
):
    try:
        return get_home_sections(limit_per_section=limit_per_section, refresh=refresh)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch anime catalog: {exc}") from exc


@router.get("/anime/search", response_model=List[AnimeItemOut])
def anime_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(18, ge=1, le=50),
    refresh: bool = Query(False),
):
    try:
        return search_home_catalog(q, limit=limit, refresh=refresh)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to search anime catalog: {exc}") from exc


@router.get("/anime/detail", response_model=AnimeDetailOut)
def anime_detail(
    url: str = Query(..., min_length=5),
    episode_limit: int = Query(40, ge=1, le=100),
):
    try:
        return get_anime_detail(url, episode_limit=episode_limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch anime detail: {exc}") from exc


@router.get("/anime/episode", response_model=AnimeEpisodeSourceOut)
def anime_episode(url: str = Query(..., min_length=5)):
    try:
        return get_episode_sources(url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch anime episode sources: {exc}") from exc


def _steam_summary_to_game(item: dict) -> dict:
    price = item.get("price") or {}
    final_price = price.get("final") if price.get("final") is not None else price.get("initial")
    price_value = (final_price or 0) / 100 if final_price else 0
    discount = price.get("discount_percent") or 0
    app_id = str(item.get("app_id") or "")
    header = item.get("header_image") or ""
    hero = item.get("background") or header
    return {
        "id": f"steam-{app_id}",
        "slug": f"steam-{app_id}",
        "steam_app_id": app_id,
        "title": item.get("name") or app_id,
        "tagline": item.get("short_description") or "",
        "short_description": item.get("short_description") or "",
        "description": item.get("short_description") or "",
        "studio": "Steam",
        "release_date": item.get("release_date") or "",
        "genres": item.get("genres") or [],
        "price": price_value,
        "discount_percent": discount,
        "rating": 0,
        "required_age": item.get("required_age"),
        "denuvo": bool(item.get("denuvo")),
        "header_image": header,
        "hero_image": hero,
        "background_image": hero,
        "screenshots": [hero] if hero else [],
        "videos": [],
        "system_requirements": None,
    }


@router.get("/similar/{game_id}", response_model=List[GameOut])
def get_similar_games(game_id: str, db: Session = Depends(get_db)):
    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return similar_games(db, game_id, limit=6)


@router.get("/recommendations", response_model=List[GameOut])
def recommendations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return recommend_games(db, current_user.id, limit=10)


@router.get("/recommendations/v2", response_model=SteamCatalogOut)
def recommendations_v2(
    limit: int = Query(12, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not AI_FEATURE_RECO_V2:
        appids = get_lua_appids()
        page_ids = appids[offset : offset + limit]
        items = get_catalog_page(page_ids)
        return {
            "total": len(appids),
            "offset": offset,
            "limit": limit,
            "items": items,
        }

    return recommend_v2(
        db,
        user_id=current_user.id,
        limit=limit,
        offset=offset,
    )
