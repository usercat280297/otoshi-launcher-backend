from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Achievement, Game, LibraryEntry, User, UserAchievement
from ..schemas import AchievementOut, AchievementUnlockIn, UserAchievementOut
from .deps import get_current_user

router = APIRouter()


@router.get("/game/{game_id}", response_model=list[AchievementOut])
def list_game_achievements(game_id: str, db: Session = Depends(get_db)):
    return db.query(Achievement).filter(Achievement.game_id == game_id).all()


@router.get("/me", response_model=list[UserAchievementOut])
def list_user_achievements(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(UserAchievement)
        .filter(UserAchievement.user_id == current_user.id)
        .all()
    )


@router.post("/unlock", response_model=UserAchievementOut)
def unlock_achievement(
    payload: AchievementUnlockIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = db.query(Game).filter(Game.id == payload.game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    owns = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.user_id == current_user.id, LibraryEntry.game_id == payload.game_id)
        .first()
    )
    if not owns:
        raise HTTPException(status_code=403, detail="Game not owned")

    achievement = (
        db.query(Achievement)
        .filter(Achievement.game_id == payload.game_id, Achievement.key == payload.achievement_key)
        .first()
    )
    if not achievement:
        raise HTTPException(status_code=404, detail="Achievement not found")

    existing = (
        db.query(UserAchievement)
        .filter(
            UserAchievement.user_id == current_user.id,
            UserAchievement.achievement_id == achievement.id,
        )
        .first()
    )
    if existing:
        return existing

    unlocked = UserAchievement(user_id=current_user.id, achievement_id=achievement.id)
    db.add(unlocked)
    db.commit()
    db.refresh(unlocked)
    return unlocked
