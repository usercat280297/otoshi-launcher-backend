from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game, LibraryEntry, PaymentTransaction, User
from ..schemas import LibraryEntryOut
from .deps import get_current_user

router = APIRouter()


@router.get("/", response_model=list[LibraryEntryOut])
def list_library(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return (
        db.query(LibraryEntry)
        .filter(LibraryEntry.user_id == current_user.id)
        .all()
    )


@router.post("/purchase/{game_id}", response_model=LibraryEntryOut)
def purchase_game(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    existing = (
        db.query(LibraryEntry)
        .filter(
            LibraryEntry.user_id == current_user.id,
            LibraryEntry.game_id == game_id
        )
        .first()
    )
    if existing:
        return existing

    entry = LibraryEntry(user_id=current_user.id, game_id=game_id)
    db.add(entry)
    amount = float(game.price) * (1 - (game.discount_percent or 0) / 100)
    db.add(
        PaymentTransaction(
            user_id=current_user.id,
            game_id=game_id,
            amount=amount,
            currency="USD",
            status="completed",
            provider="library",
        )
    )
    db.commit()
    db.refresh(entry)
    return entry


@router.post("/{entry_id}/install", response_model=LibraryEntryOut)
def mark_installed(
    entry_id: str,
    version: str = "1.0.0",
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    entry = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.id == entry_id, LibraryEntry.user_id == current_user.id)
        .first()
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Library entry not found")

    entry.installed_version = version
    db.commit()
    db.refresh(entry)
    return entry


@router.post("/{entry_id}/playtime", response_model=LibraryEntryOut)
def update_playtime(
    entry_id: str,
    hours: float,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    entry = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.id == entry_id, LibraryEntry.user_id == current_user.id)
        .first()
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Library entry not found")

    entry.playtime_hours = max(0.0, entry.playtime_hours + hours)
    db.commit()
    db.refresh(entry)
    return entry
