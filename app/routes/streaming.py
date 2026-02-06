from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import StreamingSession, User
from ..schemas import StreamingSessionOut
from .deps import get_current_user

router = APIRouter()


@router.post("/sessions", response_model=StreamingSessionOut)
def create_session(
    payload: dict = Body(default={}),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    session = StreamingSession(
        user_id=current_user.id,
        game_id=payload.get("game_id"),
        status="created",
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


@router.get("/sessions/{session_id}", response_model=StreamingSessionOut)
def get_session(session_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    session = (
        db.query(StreamingSession)
        .filter(StreamingSession.id == session_id, StreamingSession.user_id == current_user.id)
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="Streaming session not found")
    return session


@router.post("/sessions/{session_id}/offer", response_model=StreamingSessionOut)
def set_offer(
    session_id: str,
    offer: dict = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    session = (
        db.query(StreamingSession)
        .filter(StreamingSession.id == session_id, StreamingSession.user_id == current_user.id)
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="Streaming session not found")
    session.offer = offer
    session.status = "offer_set"
    db.commit()
    db.refresh(session)
    return session


@router.post("/sessions/{session_id}/answer", response_model=StreamingSessionOut)
def set_answer(
    session_id: str,
    answer: dict = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    session = (
        db.query(StreamingSession)
        .filter(StreamingSession.id == session_id, StreamingSession.user_id == current_user.id)
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="Streaming session not found")
    session.answer = answer
    session.status = "connected"
    db.commit()
    db.refresh(session)
    return session


@router.post("/sessions/{session_id}/ice", response_model=StreamingSessionOut)
def add_ice_candidate(
    session_id: str,
    candidate: dict = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    session = (
        db.query(StreamingSession)
        .filter(StreamingSession.id == session_id, StreamingSession.user_id == current_user.id)
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="Streaming session not found")
    candidates = list(session.ice_candidates or [])
    candidates.append(candidate)
    session.ice_candidates = candidates
    db.commit()
    db.refresh(session)
    return session
