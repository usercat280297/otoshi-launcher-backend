from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..core.cache import cache_client
from ..db import get_db
from ..models import Game, User
from ..schemas import GameOut
from ..core.config import DISCOVERY_FORCE_STEAM
from ..services.recommendations import recommend_games, similar_games
from ..services.steam_catalog import get_catalog_page, get_lua_appids
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
