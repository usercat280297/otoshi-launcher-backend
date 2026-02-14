from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game
from ..services.manifest import build_manifest
from ..services.v2_runtime import (
    build_repair_plan_from_report,
    run_self_heal_scan,
    runtime_v2,
)

router = APIRouter(prefix="/v2/self-heal", tags=["v2-self-heal"])


class SelfHealScanIn(BaseModel):
    install_path: str
    slug: Optional[str] = None
    version: Optional[str] = None
    channel: str = "stable"
    use_usn_delta: bool = True
    max_workers: Optional[int] = None
    manifest: Optional[dict[str, Any]] = None


class SelfHealRepairIn(BaseModel):
    report_id: Optional[str] = None
    scan_report: Optional[dict[str, Any]] = None
    slug: Optional[str] = None
    version: Optional[str] = None
    channel: str = "stable"
    install_path: Optional[str] = None
    dry_run: bool = True
    manifest: Optional[dict[str, Any]] = None


def _resolve_manifest(
    *,
    db: Session,
    slug: Optional[str],
    fallback_manifest: Optional[dict[str, Any]],
    requested_version: Optional[str],
) -> dict[str, Any]:
    if fallback_manifest:
        manifest = dict(fallback_manifest)
    else:
        if not slug:
            raise HTTPException(status_code=400, detail="slug or manifest is required")
        game = db.query(Game).filter(Game.slug == slug).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        manifest = build_manifest(game)

    if requested_version and requested_version.strip().lower() != "latest":
        manifest["version"] = requested_version.strip()
    return manifest


@router.post("/scan")
def scan_self_heal_v2(
    payload: SelfHealScanIn,
    db: Session = Depends(get_db),
):
    if not payload.install_path:
        raise HTTPException(status_code=400, detail="install_path is required")

    manifest = _resolve_manifest(
        db=db,
        slug=payload.slug,
        fallback_manifest=payload.manifest,
        requested_version=payload.version,
    )
    report = run_self_heal_scan(
        install_path=payload.install_path,
        manifest=manifest,
        max_workers=payload.max_workers,
        usn_delta_eligible=payload.use_usn_delta,
    )
    report["slug"] = payload.slug
    report["channel"] = payload.channel
    report["version"] = manifest.get("version")
    report["manifest_summary"] = {
        "total_files": len(manifest.get("files") or []),
        "chunk_size": manifest.get("chunk_size"),
    }
    saved = runtime_v2.save_scan_report(report)
    return saved


@router.post("/repair")
def repair_self_heal_v2(
    payload: SelfHealRepairIn,
    db: Session = Depends(get_db),
):
    report = payload.scan_report
    if not report and payload.report_id:
        report = runtime_v2.get_scan_report(payload.report_id)
    if not report and payload.slug and payload.install_path:
        manifest_for_scan = _resolve_manifest(
            db=db,
            slug=payload.slug,
            fallback_manifest=payload.manifest,
            requested_version=payload.version,
        )
        report = run_self_heal_scan(
            install_path=payload.install_path,
            manifest=manifest_for_scan,
            max_workers=None,
            usn_delta_eligible=True,
        )
    if not report:
        raise HTTPException(status_code=400, detail="scan_report, report_id, or slug+install_path is required")

    slug = payload.slug or report.get("slug")
    manifest = _resolve_manifest(
        db=db,
        slug=slug,
        fallback_manifest=payload.manifest,
        requested_version=payload.version,
    )
    plan = build_repair_plan_from_report(report, manifest)
    return {
        "report_id": report.get("report_id"),
        "dry_run": payload.dry_run,
        "repair_plan": plan,
        "applied": False if payload.dry_run else True,
        "message": (
            "Hot-fix queue prepared (dry run)."
            if payload.dry_run
            else "Hot-fix queue accepted for targeted repair."
        ),
    }

