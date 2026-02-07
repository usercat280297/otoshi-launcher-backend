from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from ..core.config import WORKSHOP_STORAGE_DIR, WORKSHOP_STEAM_APP_ID
from ..core.cache import cache_client
from ..services.steam_workshop import get_workshop_app_ids, query_workshop_items, query_workshop_multi
from ..db import get_db
from ..models import (
    WorkshopItem,
    WorkshopVersion,
    WorkshopSubscription,
    WorkshopRating,
    User,
)
from ..schemas import (
    WorkshopItemCreate,
    WorkshopItemOut,
    WorkshopSubscriptionOut,
    WorkshopVersionOut,
    WorkshopRatingIn,
)
from .deps import get_current_user

router = APIRouter()


def _parse_tags(tags: Optional[str]) -> List[str]:
    if not tags:
        return []
    return [tag.strip() for tag in tags.split(",") if tag.strip()]


def _item_storage_dir(item_id: str) -> Path:
    return Path(WORKSHOP_STORAGE_DIR) / item_id


@router.get("/items", response_model=List[WorkshopItemOut])
def list_items(
    game_id: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
):
    cache_key = f"workshop:items:{game_id or 'all'}:{search or ''}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached

    query = db.query(WorkshopItem)
    if game_id:
        query = query.filter(WorkshopItem.game_id == game_id)
    if search:
        query = query.filter(WorkshopItem.title.ilike(f"%{search}%"))

    items = query.order_by(WorkshopItem.updated_at.desc()).all()
    payload = [WorkshopItemOut.model_validate(item).model_dump() for item in items]
    cache_client.set_json(cache_key, payload)
    return payload


@router.get("/steam", response_model=List[WorkshopItemOut])
def list_steam_items(
    app_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 24,
):
    if app_id and app_id.lower() != "all":
        return query_workshop_items(app_id, search=search, limit=limit)

    app_ids = get_workshop_app_ids()
    return query_workshop_multi(app_ids, search=search, total_limit=limit)


@router.get("/items/{item_id}", response_model=WorkshopItemOut)
def get_item(item_id: str, db: Session = Depends(get_db)):
    item = db.query(WorkshopItem).filter(WorkshopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Workshop item not found")
    return item


@router.get("/items/{item_id}/versions", response_model=List[WorkshopVersionOut])
def list_versions(item_id: str, db: Session = Depends(get_db)):
    versions = (
        db.query(WorkshopVersion)
        .filter(WorkshopVersion.workshop_item_id == item_id)
        .order_by(WorkshopVersion.created_at.desc())
        .all()
    )
    return versions


@router.post("/items", response_model=WorkshopItemOut)
async def create_item(
    game_id: str = Form(...),
    title: str = Form(...),
    description: Optional[str] = Form(None),
    item_type: Optional[str] = Form(None),
    visibility: Optional[str] = Form("public"),
    tags: Optional[str] = Form(None),
    preview_image_url: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    payload = WorkshopItemCreate(
        game_id=game_id,
        title=title,
        description=description,
        item_type=item_type,
        visibility=visibility,
        tags=_parse_tags(tags),
        preview_image_url=preview_image_url,
    )
    item = WorkshopItem(
        game_id=payload.game_id,
        creator_id=current_user.id,
        title=payload.title,
        description=payload.description,
        item_type=payload.item_type,
        visibility=payload.visibility or "public",
        tags=payload.tags,
        preview_image_url=payload.preview_image_url,
    )
    db.add(item)
    db.commit()
    db.refresh(item)

    if file:
        content = await file.read()
        storage_dir = _item_storage_dir(item.id)
        storage_dir.mkdir(parents=True, exist_ok=True)
        storage_path = storage_dir / file.filename
        storage_path.write_bytes(content)

        version = WorkshopVersion(
            workshop_item_id=item.id,
            version="1.0.0",
            changelog="Initial upload",
            file_size=len(content),
            storage_path=str(storage_path),
            download_url=f"/workshop/items/{item.id}/versions/latest/download",
        )
        db.add(version)
        db.commit()

    cache_client.delete(f"workshop:items:{payload.game_id}:")
    cache_client.delete("workshop:items:all:")
    return item


@router.post("/items/{item_id}/versions", response_model=WorkshopVersionOut)
async def create_version(
    item_id: str,
    version: str = Form(...),
    changelog: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    item = db.query(WorkshopItem).filter(WorkshopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Workshop item not found")
    if item.creator_id != current_user.id:
        raise HTTPException(status_code=403, detail="Only the creator can upload versions")

    content = await file.read()
    storage_dir = _item_storage_dir(item.id)
    storage_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{version}-{file.filename}"
    storage_path = storage_dir / filename
    storage_path.write_bytes(content)

    version_entry = WorkshopVersion(
        workshop_item_id=item.id,
        version=version,
        changelog=changelog,
        file_size=len(content),
        storage_path=str(storage_path),
        download_url=f"/workshop/items/{item.id}/versions/{version}/download",
    )
    db.add(version_entry)
    db.commit()
    db.refresh(version_entry)
    return version_entry


@router.get("/items/{item_id}/versions/{version}/download")
def download_version(item_id: str, version: str, db: Session = Depends(get_db)):
    item = db.query(WorkshopItem).filter(WorkshopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Workshop item not found")

    if version == "latest":
        version_entry = (
            db.query(WorkshopVersion)
            .filter(WorkshopVersion.workshop_item_id == item_id)
            .order_by(WorkshopVersion.created_at.desc())
            .first()
        )
    else:
        version_entry = (
            db.query(WorkshopVersion)
            .filter(
                WorkshopVersion.workshop_item_id == item_id,
                WorkshopVersion.version == version,
            )
            .first()
        )

    if not version_entry or not version_entry.storage_path:
        raise HTTPException(status_code=404, detail="Version not found")

    if not os.path.isfile(version_entry.storage_path):
        raise HTTPException(status_code=404, detail="File missing on server")

    item.total_downloads = (item.total_downloads or 0) + 1
    db.commit()
    return FileResponse(version_entry.storage_path, filename=os.path.basename(version_entry.storage_path))


@router.post("/items/{item_id}/subscribe", response_model=WorkshopSubscriptionOut)
def subscribe_item(
    item_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    item = db.query(WorkshopItem).filter(WorkshopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Workshop item not found")

    existing = (
        db.query(WorkshopSubscription)
        .filter(
            WorkshopSubscription.user_id == current_user.id,
            WorkshopSubscription.workshop_item_id == item_id,
        )
        .first()
    )
    if existing:
        return existing

    subscription = WorkshopSubscription(
        user_id=current_user.id,
        workshop_item_id=item_id,
        auto_update=True,
    )
    db.add(subscription)
    item.total_subscriptions = (item.total_subscriptions or 0) + 1
    db.commit()
    db.refresh(subscription)
    return subscription


@router.delete("/items/{item_id}/subscribe")
def unsubscribe_item(
    item_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    subscription = (
        db.query(WorkshopSubscription)
        .filter(
            WorkshopSubscription.user_id == current_user.id,
            WorkshopSubscription.workshop_item_id == item_id,
        )
        .first()
    )
    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found")

    item = db.query(WorkshopItem).filter(WorkshopItem.id == item_id).first()
    if item:
        item.total_subscriptions = max(0, (item.total_subscriptions or 0) - 1)
    db.delete(subscription)
    db.commit()
    return {"status": "unsubscribed"}


@router.get("/subscriptions", response_model=List[WorkshopSubscriptionOut])
def list_subscriptions(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(WorkshopSubscription)
        .filter(WorkshopSubscription.user_id == current_user.id)
        .all()
    )


@router.post("/items/{item_id}/rating")
def rate_item(
    item_id: str,
    payload: WorkshopRatingIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    item = db.query(WorkshopItem).filter(WorkshopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Workshop item not found")

    rating = (
        db.query(WorkshopRating)
        .filter(
            WorkshopRating.user_id == current_user.id,
            WorkshopRating.workshop_item_id == item_id,
        )
        .first()
    )
    if rating:
        if rating.rating != payload.rating:
            if rating.rating:
                item.rating_up = max(0, (item.rating_up or 0) - 1)
            else:
                item.rating_down = max(0, (item.rating_down or 0) - 1)
            rating.rating = payload.rating
    else:
        rating = WorkshopRating(
            user_id=current_user.id,
            workshop_item_id=item_id,
            rating=payload.rating,
        )
        db.add(rating)

    if payload.rating:
        item.rating_up = (item.rating_up or 0) + 1
    else:
        item.rating_down = (item.rating_down or 0) + 1

    db.commit()
    return {"rating_up": item.rating_up, "rating_down": item.rating_down}
