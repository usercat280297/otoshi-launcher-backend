from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import PrivacyConsent, User
from ..schemas import (
    PrivacyConsentIn,
    PrivacyConsentOut,
    PrivacyDeleteIn,
    PrivacyDeleteOut,
    PrivacyExportOut,
)
from ..services.privacy import (
    PRIVACY_CATEGORIES,
    delete_user_ai_data,
    export_user_ai_data,
    normalize_privacy_categories,
    upsert_consent,
)
from .deps import get_current_user

router = APIRouter()


@router.post("/consent", response_model=list[PrivacyConsentOut])
def set_privacy_consent(
    payload: PrivacyConsentIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    categories = list(payload.categories or [])
    if payload.category:
        categories.append(payload.category)
    normalized_categories = normalize_privacy_categories(categories)
    if not normalized_categories:
        supported = ", ".join(sorted(PRIVACY_CATEGORIES))
        raise HTTPException(status_code=400, detail=f"No valid category provided. Supported: {supported}")

    rows = []
    for category in normalized_categories:
        row = upsert_consent(
            db,
            user_id=current_user.id,
            category=category,
            granted=payload.granted,
            source=payload.source,
            payload=payload.payload,
        )
        rows.append(row)
    db.commit()
    return rows


@router.get("/consent", response_model=list[PrivacyConsentOut])
def list_privacy_consent(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(PrivacyConsent)
        .filter(PrivacyConsent.user_id == current_user.id)
        .order_by(PrivacyConsent.category.asc())
        .all()
    )


@router.get("/export", response_model=PrivacyExportOut)
def export_privacy_data(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    payload = export_user_ai_data(db, current_user.id)
    return payload


@router.delete("/data", response_model=PrivacyDeleteOut)
def delete_privacy_data(
    payload: PrivacyDeleteIn | None = Body(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    row, deleted_counts = delete_user_ai_data(
        db,
        user_id=current_user.id,
        scope=(payload.scope if payload else []),
    )
    db.commit()
    return PrivacyDeleteOut(
        request_id=row.id,
        status=row.status,
        deleted_counts=deleted_counts,
    )
