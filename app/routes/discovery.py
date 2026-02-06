from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..core.cache import cache_client
from ..db import get_db
from ..models import Game, User
from ..schemas import GameOut
from ..services.recommendations import recommend_games, similar_games
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
    cache_client.set_json(cache_key, payload, ttl=300)
    return queue


@router.post("/queue/refresh", response_model=List[GameOut])
def refresh_discovery_queue(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    cache_client.delete(f"discovery:queue:{current_user.id}")
    queue = recommend_games(db, current_user.id, limit=12)
    return queue


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
