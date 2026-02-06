from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game, RemoteDownload, User
from ..schemas import RemoteDownloadIn, RemoteDownloadOut
from .deps import get_current_user

router = APIRouter()


@router.get("/", response_model=List[RemoteDownloadOut])
def list_remote_downloads(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(RemoteDownload)
        .filter(RemoteDownload.user_id == current_user.id)
        .order_by(RemoteDownload.created_at.desc())
        .all()
    )


@router.post("/queue", response_model=RemoteDownloadOut)
def queue_remote_download(
    payload: RemoteDownloadIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = db.query(Game).filter(Game.id == payload.game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    item = RemoteDownload(
        user_id=current_user.id,
        game_id=payload.game_id,
        target_device=payload.target_device,
        status="queued",
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.get("/pending", response_model=List[RemoteDownloadOut])
def pending_downloads(
    target_device: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = db.query(RemoteDownload).filter(RemoteDownload.user_id == current_user.id)
    if target_device:
        query = query.filter(RemoteDownload.target_device == target_device)
    return query.filter(RemoteDownload.status.in_(["queued", "downloading"])).all()


@router.post("/{download_id}/status", response_model=RemoteDownloadOut)
def update_status(
    download_id: str,
    status: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    item = (
        db.query(RemoteDownload)
        .filter(RemoteDownload.id == download_id, RemoteDownload.user_id == current_user.id)
        .first()
    )
    if not item:
        raise HTTPException(status_code=404, detail="Remote download not found")
    item.status = status
    db.commit()
    db.refresh(item)
    return item
