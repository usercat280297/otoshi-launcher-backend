from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game, WishlistEntry, User
from ..schemas import WishlistEntryOut
from .deps import get_current_user

router = APIRouter()


@router.get("/", response_model=List[WishlistEntryOut])
def list_wishlist(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(WishlistEntry)
        .filter(WishlistEntry.user_id == current_user.id)
        .all()
    )


@router.post("/{game_id}", response_model=WishlistEntryOut)
def add_to_wishlist(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    existing = (
        db.query(WishlistEntry)
        .filter(WishlistEntry.user_id == current_user.id, WishlistEntry.game_id == game_id)
        .first()
    )
    if existing:
        return existing

    entry = WishlistEntry(user_id=current_user.id, game_id=game_id)
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return entry


@router.delete("/{game_id}")
def remove_from_wishlist(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    entry = (
        db.query(WishlistEntry)
        .filter(WishlistEntry.user_id == current_user.id, WishlistEntry.game_id == game_id)
        .first()
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Wishlist entry not found")
    db.delete(entry)
    db.commit()
    return {"status": "removed"}
