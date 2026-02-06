from typing import List

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Bundle, DlcItem, Game, Preorder, User
from ..schemas import BundleOut, DlcOut, PreorderOut
from .deps import get_current_user

router = APIRouter()


def require_developer(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role not in ("developer", "admin"):
        raise HTTPException(status_code=403, detail="Developer access required")
    return current_user


@router.get("/bundles", response_model=List[BundleOut])
def list_bundles(db: Session = Depends(get_db)):
    return db.query(Bundle).order_by(Bundle.created_at.desc()).all()


@router.get("/bundles/{bundle_id}", response_model=BundleOut)
def get_bundle(bundle_id: str, db: Session = Depends(get_db)):
    bundle = db.query(Bundle).filter(Bundle.id == bundle_id).first()
    if not bundle:
        raise HTTPException(status_code=404, detail="Bundle not found")
    return bundle


@router.post("/bundles", response_model=BundleOut)
def create_bundle(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_developer),
):
    bundle = Bundle(
        slug=payload.get("slug", "").strip(),
        title=payload.get("title", "").strip(),
        description=payload.get("description"),
        price=float(payload.get("price", 0.0)),
        discount_percent=int(payload.get("discount_percent", 0)),
        game_ids=payload.get("game_ids", []),
    )
    if not bundle.slug or not bundle.title:
        raise HTTPException(status_code=400, detail="Missing bundle slug or title")
    db.add(bundle)
    db.commit()
    db.refresh(bundle)
    return bundle


@router.get("/dlc/{game_id}", response_model=List[DlcOut])
def list_dlc(game_id: str, db: Session = Depends(get_db)):
    return db.query(DlcItem).filter(DlcItem.base_game_id == game_id).all()


@router.post("/dlc", response_model=DlcOut)
def create_dlc(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_developer),
):
    base_game_id = payload.get("base_game_id")
    if not base_game_id:
        raise HTTPException(status_code=400, detail="Missing base_game_id")
    dlc = DlcItem(
        base_game_id=base_game_id,
        title=payload.get("title", "").strip(),
        description=payload.get("description"),
        price=float(payload.get("price", 0.0)),
        is_season_pass=bool(payload.get("is_season_pass", False)),
        release_date=payload.get("release_date"),
    )
    if not dlc.title:
        raise HTTPException(status_code=400, detail="Missing dlc title")
    db.add(dlc)
    db.commit()
    db.refresh(dlc)
    return dlc


@router.get("/preorders/me", response_model=List[PreorderOut])
def list_preorders(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(Preorder)
        .filter(Preorder.user_id == current_user.id)
        .order_by(Preorder.preorder_at.desc())
        .all()
    )


@router.post("/preorders/{game_id}", response_model=PreorderOut)
def preorder_game(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    existing = (
        db.query(Preorder)
        .filter(Preorder.user_id == current_user.id, Preorder.game_id == game_id)
        .first()
    )
    if existing:
        return existing

    preorder = Preorder(user_id=current_user.id, game_id=game_id)
    db.add(preorder)
    db.commit()
    db.refresh(preorder)
    return preorder
