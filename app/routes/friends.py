from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Friendship, User
from ..schemas import FriendRequestIn, FriendshipOut
from .deps import get_current_user

router = APIRouter()


@router.get("/", response_model=list[FriendshipOut])
def list_friends(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(Friendship)
        .filter(Friendship.user_id == current_user.id)
        .order_by(Friendship.updated_at.desc())
        .all()
    )


@router.get("/requests", response_model=list[FriendshipOut])
def list_friend_requests(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(Friendship)
        .filter(Friendship.friend_id == current_user.id, Friendship.status == "pending")
        .order_by(Friendship.created_at.desc())
        .all()
    )


@router.post("/request", response_model=FriendshipOut)
def send_request(
    payload: FriendRequestIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    target = db.query(User).filter(User.username == payload.target_username).first()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target.id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot friend yourself")

    existing = (
        db.query(Friendship)
        .filter(
            Friendship.user_id == current_user.id,
            Friendship.friend_id == target.id,
        )
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Already requested")

    friendship = Friendship(
        user_id=current_user.id,
        friend_id=target.id,
        status="pending",
    )
    db.add(friendship)
    db.commit()
    db.refresh(friendship)

    return friendship


@router.post("/{friendship_id}/accept", response_model=FriendshipOut)
def accept_request(
    friendship_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    friendship = (
        db.query(Friendship)
        .filter(Friendship.id == friendship_id, Friendship.friend_id == current_user.id)
        .first()
    )
    if not friendship:
        raise HTTPException(status_code=404, detail="Request not found")

    friendship.status = "accepted"
    db.commit()
    db.refresh(friendship)
    return friendship


@router.delete("/{friendship_id}")
def remove_friend(
    friendship_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    friendship = (
        db.query(Friendship)
        .filter(
            Friendship.id == friendship_id,
            (Friendship.user_id == current_user.id) | (Friendship.friend_id == current_user.id),
        )
        .first()
    )
    if not friendship:
        raise HTTPException(status_code=404, detail="Friendship not found")

    db.delete(friendship)
    db.commit()
    return {"status": "removed"}
