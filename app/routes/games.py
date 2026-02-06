from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..core.cache import cache_client
from ..models import Game
from ..schemas import GameOut
from ..middleware.cache import cache_response

router = APIRouter()


@router.get("/", response_model=List[GameOut])
def list_games(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None),
    genre: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    cache_key = f"games:list:page={page}:limit={limit}:search={search or ''}:genre={genre or ''}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached
    query = db.query(Game).filter(Game.is_published == True)
    if search:
        query = query.filter(Game.title.ilike(f"%{search}%"))
    if genre:
        query = query.filter(Game.genres.contains([genre]))
    results = query.offset((page - 1) * limit).limit(limit).all()
    cache_client.set_json(cache_key, [GameOut.model_validate(game).model_dump() for game in results])
    return results


@router.get("/popular", response_model=List[GameOut])
@cache_response(ttl=600)
def popular_games(db: Session = Depends(get_db)):
    games = (
        db.query(Game)
        .filter(Game.is_published == True)
        .order_by(Game.total_downloads.desc(), Game.rating.desc())
        .limit(10)
        .all()
    )
    return [GameOut.model_validate(game).model_dump() for game in games]


@router.get("/{slug}", response_model=GameOut)
def get_game(slug: str, db: Session = Depends(get_db)):
    cache_key = f"games:detail:{slug}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached
    game = db.query(Game).filter(Game.slug == slug).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    cache_client.set_json(cache_key, GameOut.model_validate(game).model_dump())
    return game
