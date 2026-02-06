from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game, LibraryEntry, License, User
from ..schemas import LicenseIssueIn, LicenseOut, SignedLicense
from datetime import timezone
from ..services.license import get_public_key_pem, issue_license
from .deps import get_current_user

router = APIRouter()


@router.get("/public-key")
def public_key() -> dict:
    return {"public_key": get_public_key_pem()}


@router.post("/issue", response_model=SignedLicense)
def create_license(
    payload: LicenseIssueIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = db.query(Game).filter(Game.id == payload.game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    owns = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.user_id == current_user.id, LibraryEntry.game_id == game.id)
        .first()
    )
    if not owns:
        raise HTTPException(status_code=403, detail="Game not owned")

    existing = (
        db.query(License)
        .filter(License.user_id == current_user.id, License.game_id == game.id)
        .first()
    )
    if existing:
        return SignedLicense(
            license_id=existing.id,
            user_id=existing.user_id,
            game_id=existing.game_id,
            issued_at=existing.issued_at.replace(tzinfo=timezone.utc).isoformat(),
            expires_at=existing.expires_at.replace(tzinfo=timezone.utc).isoformat()
            if existing.expires_at
            else None,
            max_activations=existing.max_activations,
            current_activations=existing.current_activations,
            hardware_id=existing.hardware_id,
            signature=existing.signature or "",
        )

    license_item = issue_license(
        current_user.id,
        game.id,
        payload.hardware_id,
        payload.expires_at,
        payload.max_activations,
    )
    db.add(license_item)
    db.commit()
    db.refresh(license_item)
    return SignedLicense(
        license_id=license_item.id,
        user_id=license_item.user_id,
        game_id=license_item.game_id,
        issued_at=license_item.issued_at.replace(tzinfo=timezone.utc).isoformat(),
        expires_at=license_item.expires_at.replace(tzinfo=timezone.utc).isoformat()
        if license_item.expires_at
        else None,
        max_activations=license_item.max_activations,
        current_activations=license_item.current_activations,
        hardware_id=license_item.hardware_id,
        signature=license_item.signature or "",
    )


@router.get("/me", response_model=list[LicenseOut])
def list_licenses(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return db.query(License).filter(License.user_id == current_user.id).all()


@router.post("/{license_id}/activate", response_model=LicenseOut)
def activate_license(
    license_id: str,
    hardware_id: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    license_item = (
        db.query(License)
        .filter(License.id == license_id, License.user_id == current_user.id)
        .first()
    )
    if not license_item:
        raise HTTPException(status_code=404, detail="License not found")
    if license_item.status != "active":
        raise HTTPException(status_code=400, detail="License inactive")
    if license_item.current_activations >= license_item.max_activations:
        raise HTTPException(status_code=400, detail="Activation limit reached")

    if hardware_id:
        license_item.hardware_id = hardware_id
    license_item.current_activations += 1
    db.commit()
    db.refresh(license_item)
    return license_item
