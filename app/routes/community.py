from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from ..core.config import SCREENSHOT_STORAGE_DIR
from ..db import get_db
from ..models import (
    ActivityEvent,
    Friendship,
    Review,
    ReviewVote,
    Screenshot,
    User,
    UserProfile,
)
from ..schemas import (
    ActivityEventOut,
    ReviewIn,
    ReviewOut,
    ScreenshotOut,
    UserProfileOut,
    UserProfileUpdate,
    UserPublicOut,
)
from .deps import get_current_user

router = APIRouter()


def _emit_activity(db: Session, user_id: str, event_type: str, payload: dict) -> None:
    db.add(ActivityEvent(user_id=user_id, event_type=event_type, payload=payload))


@router.get("/profile/{user_id}")
def get_profile(user_id: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    profile = db.query(UserProfile).filter(UserProfile.user_id == user_id).first()
    return {
        "user": UserPublicOut.model_validate(user).model_dump(),
        "profile": UserProfileOut.model_validate(profile).model_dump() if profile else None,
    }


@router.get("/profile/me")
def get_my_profile(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    profile = db.query(UserProfile).filter(UserProfile.user_id == current_user.id).first()
    return {
        "user": UserPublicOut.model_validate(current_user).model_dump(),
        "profile": UserProfileOut.model_validate(profile).model_dump() if profile else None,
    }


@router.post("/profile", response_model=UserProfileOut)
def update_profile(
    payload: UserProfileUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    profile = db.query(UserProfile).filter(UserProfile.user_id == current_user.id).first()
    if not profile:
        profile = UserProfile(user_id=current_user.id)
        db.add(profile)

    if payload.headline is not None:
        profile.headline = payload.headline
    if payload.bio is not None:
        profile.bio = payload.bio
    if payload.location is not None:
        profile.location = payload.location
    if payload.social_links is not None:
        profile.social_links = payload.social_links

    db.commit()
    db.refresh(profile)
    return profile


@router.get("/reviews/{game_id}", response_model=List[ReviewOut])
def list_reviews(game_id: str, db: Session = Depends(get_db)):
    return (
        db.query(Review)
        .filter(Review.game_id == game_id)
        .order_by(Review.created_at.desc())
        .all()
    )


@router.post("/reviews/{game_id}", response_model=ReviewOut)
def create_review(
    game_id: str,
    payload: ReviewIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    review = (
        db.query(Review)
        .filter(Review.user_id == current_user.id, Review.game_id == game_id)
        .first()
    )
    if not review:
        review = Review(user_id=current_user.id, game_id=game_id)
        db.add(review)

    review.rating = payload.rating
    review.title = payload.title
    review.body = payload.body
    review.recommended = payload.recommended
    db.commit()
    db.refresh(review)

    _emit_activity(
        db,
        current_user.id,
        "review_posted",
        {"game_id": game_id, "rating": payload.rating},
    )
    db.commit()
    return review


@router.post("/reviews/{review_id}/helpful", response_model=ReviewOut)
def mark_helpful(
    review_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    review = db.query(Review).filter(Review.id == review_id).first()
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    existing = (
        db.query(ReviewVote)
        .filter(ReviewVote.review_id == review_id, ReviewVote.user_id == current_user.id)
        .first()
    )
    if existing:
        return review

    db.add(ReviewVote(user_id=current_user.id, review_id=review_id, helpful=True))
    review.helpful_count = (review.helpful_count or 0) + 1
    db.commit()
    db.refresh(review)
    return review


@router.get("/screenshots/{game_id}", response_model=List[ScreenshotOut])
def list_screenshots(game_id: str, db: Session = Depends(get_db)):
    return (
        db.query(Screenshot)
        .filter(Screenshot.game_id == game_id)
        .order_by(Screenshot.created_at.desc())
        .all()
    )


@router.post("/screenshots", response_model=ScreenshotOut)
async def upload_screenshot(
    game_id: str = Form(...),
    caption: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    storage_dir = Path(SCREENSHOT_STORAGE_DIR) / current_user.id
    storage_dir.mkdir(parents=True, exist_ok=True)
    target_path = storage_dir / file.filename
    content = await file.read()
    target_path.write_bytes(content)

    screenshot = Screenshot(
        user_id=current_user.id,
        game_id=game_id,
        image_url=f"/community/screenshots/file/{current_user.id}/{file.filename}",
        caption=caption,
    )
    db.add(screenshot)
    _emit_activity(
        db,
        current_user.id,
        "screenshot_shared",
        {"game_id": game_id, "caption": caption or ""},
    )
    db.commit()
    db.refresh(screenshot)
    return screenshot


@router.get("/screenshots/file/{user_id}/{filename}")
def get_screenshot_file(user_id: str, filename: str):
    path = Path(SCREENSHOT_STORAGE_DIR) / user_id / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Screenshot not found")
    return FileResponse(path, filename=filename)


@router.get("/activity", response_model=List[ActivityEventOut])
def activity_feed(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    friend_ids = set()
    friendships = (
        db.query(Friendship)
        .filter(Friendship.status == "accepted")
        .filter(
            (Friendship.user_id == current_user.id)
            | (Friendship.friend_id == current_user.id)
        )
        .all()
    )
    for friendship in friendships:
        if friendship.user_id != current_user.id:
            friend_ids.add(friendship.user_id)
        if friendship.friend_id != current_user.id:
            friend_ids.add(friendship.friend_id)

    ids = list(friend_ids | {current_user.id})
    return (
        db.query(ActivityEvent)
        .filter(ActivityEvent.user_id.in_(ids))
        .order_by(ActivityEvent.created_at.desc())
        .limit(50)
        .all()
    )


@router.post("/activity", response_model=ActivityEventOut)
def emit_activity(
    event_type: str = Form(...),
    payload: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    event = ActivityEvent(
        user_id=current_user.id,
        event_type=event_type,
        payload={"message": payload or ""},
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event
