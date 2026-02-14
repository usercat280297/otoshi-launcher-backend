from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game, GamePlaySession, LibraryEntry, PaymentTransaction, User
from ..schemas import (
    LibraryEntryOut,
    LibraryPlaySessionIn,
    LibraryPlaySessionOut,
    LibraryPlaytimeIn,
    LibraryPlaytimeOut,
)
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


@router.post("/{entry_id}/playtime", response_model=LibraryPlaytimeOut)
def update_playtime(
    entry_id: str,
    payload: Optional[LibraryPlaytimeIn] = None,
    hours: Optional[float] = None,
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

    delta_hours = 0.0
    if payload is not None:
        delta_hours = max(0.0, float(payload.duration_sec) / 3600.0)
    elif hours is not None:
        delta_hours = max(0.0, float(hours))

    entry.playtime_hours = max(0.0, float(entry.playtime_hours or 0.0) + delta_hours)
    entry.last_played_at = datetime.utcnow()
    db.commit()
    db.refresh(entry)
    return LibraryPlaytimeOut(
        entry_id=entry.id,
        game_id=entry.game_id,
        playtime_hours=entry.playtime_hours,
        last_played_at=entry.last_played_at,
    )


@router.post("/{entry_id}/session", response_model=LibraryPlaySessionOut)
def record_play_session(
    entry_id: str,
    payload: LibraryPlaySessionIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    entry = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.id == entry_id, LibraryEntry.user_id == current_user.id)
        .first()
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Library entry not found")

    started_at = payload.started_at or datetime.utcnow()
    ended_at = payload.ended_at

    if payload.duration_sec is not None:
        duration_sec = max(0, int(payload.duration_sec))
    elif ended_at:
        duration_sec = max(0, int((ended_at - started_at).total_seconds()))
    else:
        duration_sec = 0

    if ended_at is None and duration_sec > 0:
        ended_at = datetime.utcfromtimestamp(started_at.timestamp() + duration_sec)

    session = GamePlaySession(
        user_id=current_user.id,
        game_id=entry.game_id,
        started_at=started_at,
        ended_at=ended_at,
        duration_sec=duration_sec,
        exit_code=payload.exit_code,
    )
    db.add(session)

    if duration_sec > 0:
        entry.playtime_hours = max(0.0, float(entry.playtime_hours or 0.0) + (duration_sec / 3600.0))
    entry.last_played_at = ended_at or started_at

    db.commit()
    db.refresh(session)
    return session
