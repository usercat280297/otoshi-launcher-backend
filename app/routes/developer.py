from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from ..core.config import BUILD_STORAGE_DIR, ADMIN_ONLY_DEVELOPER_PORTAL
from ..db import get_db
from ..models import (
    DeveloperAnalyticsSnapshot,
    DeveloperBuild,
    DeveloperDepot,
    Game,
    LibraryEntry,
    PaymentTransaction,
    User,
)
from ..schemas import DeveloperAnalyticsOut, DeveloperBuildOut, DeveloperDepotOut
from .deps import get_current_user
from ..utils.admin import is_admin_identity

router = APIRouter()


def require_developer(current_user: User = Depends(get_current_user)) -> User:
    if ADMIN_ONLY_DEVELOPER_PORTAL:
        if not is_admin_identity(current_user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return current_user
    if current_user.role not in ("developer", "admin"):
        raise HTTPException(status_code=403, detail="Developer access required")
    return current_user


@router.get("/analytics", response_model=List[DeveloperAnalyticsOut])
def analytics(
    game_id: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_developer),
):
    query = db.query(Game)
    if current_user.role != "admin":
        query = query.filter(Game.developer == current_user.username)
    if game_id:
        query = query.filter(Game.id == game_id)
    games = query.all()

    snapshots: List[DeveloperAnalyticsSnapshot] = []
    for game in games:
        payments = db.query(PaymentTransaction).filter(PaymentTransaction.game_id == game.id).all()
        library_entries = db.query(LibraryEntry).filter(LibraryEntry.game_id == game.id).all()
        metrics = {
            "total_sales": round(sum(payment.amount for payment in payments), 2),
            "total_transactions": len(payments),
            "library_count": len(library_entries),
            "total_downloads": game.total_downloads or 0,
            "rating": game.rating or 0.0,
        }
        snapshot = DeveloperAnalyticsSnapshot(game_id=game.id, metrics=metrics)
        db.add(snapshot)
        snapshots.append(snapshot)

    db.commit()
    return snapshots


@router.get("/games/{game_id}/depots", response_model=List[DeveloperDepotOut])
def list_depots(
    game_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_developer),
):
    return db.query(DeveloperDepot).filter(DeveloperDepot.game_id == game_id).all()


@router.post("/games/{game_id}/depots", response_model=DeveloperDepotOut)
def create_depot(
    game_id: str,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_developer),
):
    depot = DeveloperDepot(
        game_id=game_id,
        name=payload.get("name", "").strip(),
        platform=payload.get("platform", "windows"),
        branch=payload.get("branch", "main"),
    )
    if not depot.name:
        raise HTTPException(status_code=400, detail="Depot name is required")
    db.add(depot)
    db.commit()
    db.refresh(depot)
    return depot


@router.get("/depots/{depot_id}/builds", response_model=List[DeveloperBuildOut])
def list_builds(
    depot_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_developer),
):
    return db.query(DeveloperBuild).filter(DeveloperBuild.depot_id == depot_id).all()


@router.post("/depots/{depot_id}/builds", response_model=DeveloperBuildOut)
async def upload_build(
    depot_id: str,
    version: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_developer),
):
    depot = db.query(DeveloperDepot).filter(DeveloperDepot.id == depot_id).first()
    if not depot:
        raise HTTPException(status_code=404, detail="Depot not found")

    storage_dir = Path(BUILD_STORAGE_DIR) / depot_id
    storage_dir.mkdir(parents=True, exist_ok=True)
    target_path = storage_dir / file.filename
    content = await file.read()
    target_path.write_bytes(content)

    manifest = {
        "version": version,
        "file_name": file.filename,
        "file_size": len(content),
        "platform": depot.platform,
        "branch": depot.branch,
    }

    build = DeveloperBuild(
        depot_id=depot_id,
        version=version,
        manifest_json=manifest,
    )
    db.add(build)
    db.commit()
    db.refresh(build)
    return build
