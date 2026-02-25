from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import SupportDonation, User
from ..schemas import SupportProfileOut, UserOut, UserPublicOut, UserUpdate
from .deps import get_current_user

router = APIRouter()


@router.get("/me", response_model=UserOut)
def get_me(current_user: User = Depends(get_current_user)):
    return current_user


@router.get("/me/support", response_model=SupportProfileOut)
def get_my_support_profile(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    lifetime_total = (
        db.query(func.coalesce(func.sum(SupportDonation.amount), 0.0))
        .filter(SupportDonation.user_id == current_user.id)
        .scalar()
    )
    period_since = datetime.utcnow() - timedelta(days=30)
    period_total = (
        db.query(func.coalesce(func.sum(SupportDonation.amount), 0.0))
        .filter(
            SupportDonation.user_id == current_user.id,
            SupportDonation.created_at >= period_since,
        )
        .scalar()
    )
    currency = (
        db.query(SupportDonation.currency)
        .filter(SupportDonation.user_id == current_user.id)
        .order_by(SupportDonation.created_at.desc())
        .limit(1)
        .scalar()
    ) or "USD"

    ranking_rows = (
        db.query(
            SupportDonation.user_id.label("user_id"),
            func.coalesce(func.sum(SupportDonation.amount), 0.0).label("total_amount"),
        )
        .group_by(SupportDonation.user_id)
        .order_by(func.sum(SupportDonation.amount).desc())
        .all()
    )
    rank: int | None = None
    for index, row in enumerate(ranking_rows, start=1):
        if str(row.user_id) == current_user.id:
            rank = index
            break

    return {
        "tier": current_user.membership_tier,
        "expires_at": current_user.membership_expires_at,
        "lifetime_total": float(lifetime_total or 0.0),
        "period_total": float(period_total or 0.0),
        "rank": rank,
        "currency": str(currency or "USD"),
    }


@router.patch("/me", response_model=UserOut)
def update_me(
    payload: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if payload.display_name is not None:
        current_user.display_name = payload.display_name
    if payload.avatar_url is not None:
        current_user.avatar_url = payload.avatar_url
    db.commit()
    db.refresh(current_user)
    return current_user


@router.get("/{user_id}", response_model=UserPublicOut)
def get_user_profile(user_id: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user
