from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game
from ..services.manifest import build_manifest
from ..services.v2_runtime import _stable_sha256

router = APIRouter(prefix="/v2/manifests", tags=["v2-manifests"])


class ManifestIntegrityOut(BaseModel):
    algorithm: str
    canonical_hash: str


class ManifestV2Out(BaseModel):
    schema_version: str
    slug: str
    channel: str
    version: str
    generated_at: str
    integrity: ManifestIntegrityOut
    manifest: dict


@router.get("/{slug}", response_model=ManifestV2Out)
def get_manifest_v2(
    slug: str,
    version: Optional[str] = Query(default=None),
    channel: str = Query(default="stable"),
    db: Session = Depends(get_db),
) -> ManifestV2Out:
    game = db.query(Game).filter(Game.slug == slug).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    manifest = build_manifest(game)
    requested_version = (version or "").strip()
    if requested_version and requested_version.lower() != "latest":
        manifest["version"] = requested_version
        manifest["requested_version"] = requested_version

    canonical_hash = _stable_sha256(manifest)
    payload = ManifestV2Out(
        schema_version="2.0",
        slug=slug,
        channel=(channel or "stable").strip().lower(),
        version=str(manifest.get("version") or "latest"),
        generated_at=datetime.now(timezone.utc).isoformat(),
        integrity=ManifestIntegrityOut(
            algorithm="SHA-256",
            canonical_hash=canonical_hash,
        ),
        manifest=manifest,
    )
    return payload

