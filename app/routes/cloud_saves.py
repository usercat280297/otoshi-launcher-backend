from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import CloudSave, Game, LibraryEntry, User
from ..schemas import CloudSaveIn, CloudSaveOut
from .deps import get_current_user

router = APIRouter()


@router.get("/{game_id}", response_model=CloudSaveOut)
def get_cloud_save(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    save = (
        db.query(CloudSave)
        .filter(CloudSave.user_id == current_user.id, CloudSave.game_id == game_id)
        .first()
    )
    if not save:
        raise HTTPException(status_code=404, detail="Save not found")
    return save


@router.post("/", response_model=CloudSaveOut)
def upload_cloud_save(
    payload: CloudSaveIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = db.query(Game).filter(Game.id == payload.game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    owns = (
        db.query(LibraryEntry)
        .filter(
            LibraryEntry.user_id == current_user.id,
            LibraryEntry.game_id == payload.game_id,
        )
        .first()
    )
    if not owns:
        raise HTTPException(status_code=403, detail="Game not owned")

    save = (
        db.query(CloudSave)
        .filter(CloudSave.user_id == current_user.id, CloudSave.game_id == payload.game_id)
        .first()
    )
    if save:
        save.payload = payload.payload
        if payload.version:
            save.version = payload.version
    else:
        save = CloudSave(
            user_id=current_user.id,
            game_id=payload.game_id,
            payload=payload.payload,
            version=payload.version or "1",
        )
        db.add(save)

    db.commit()
    db.refresh(save)
    return save
