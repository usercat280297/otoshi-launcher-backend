from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import DownloadTask, Game, User
from ..routes.deps import get_current_user
from ..services.download_options import build_download_options
from ..services.steam_catalog import get_catalog_page, get_steam_detail, get_steam_summary
from ..services.v2_runtime import runtime_v2

router = APIRouter(prefix="/v2/download-sessions", tags=["v2-downloads"])


class DownloadSessionCreateIn(BaseModel):
    slug: Optional[str] = None
    game_id: Optional[str] = None
    app_id: Optional[str] = None
    version: str = "latest"
    channel: str = "stable"
    method: str = "chunks"
    install_path: Optional[str] = None
    create_subfolder: bool = True


class DownloadSessionControlIn(BaseModel):
    action: str = Field(pattern="^(pause|resume|cancel)$")


def _first_or_none(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        for item in value:
            if item:
                return item
        return None
    return value if value else None


def _resolve_steam_metadata(app_id: str) -> tuple[Optional[dict], Optional[dict]]:
    summary = get_steam_summary(app_id)
    detail = get_steam_detail(app_id)

    if not summary:
        page = get_catalog_page([app_id])
        if page:
            summary = page[0]

    if not detail and summary:
        detail = summary

    if detail:
        return summary, detail

    options = build_download_options(app_id)
    if not options:
        return summary, None

    name = options.get("name") or f"Steam App {app_id}"
    fallback_desc = "Chunk manifest mapped title"
    if options.get("size_label"):
        fallback_desc = f"{fallback_desc} ({options.get('size_label')})"

    fallback_detail = {
        "app_id": str(app_id),
        "name": name,
        "short_description": fallback_desc,
        "developers": [],
        "publishers": [],
        "release_date": None,
        "genres": [],
        "platforms": ["windows"],
        "header_image": None,
        "background": None,
        "screenshots": [],
        "movies": [],
        "pc_requirements": None,
    }

    if not summary:
        summary = {
            "app_id": str(app_id),
            "name": name,
            "short_description": fallback_desc,
            "genres": [],
            "platforms": ["windows"],
            "release_date": None,
            "header_image": None,
            "background": None,
        }

    return summary, fallback_detail


def _ensure_game(
    payload: DownloadSessionCreateIn,
    db: Session,
) -> Game:
    if payload.game_id:
        game = db.query(Game).filter(Game.id == payload.game_id).first()
        if game:
            return game

    if payload.slug:
        game = db.query(Game).filter(Game.slug == payload.slug).first()
        if game:
            return game

    app_id = (payload.app_id or "").strip()
    if app_id:
        slug = f"steam-{app_id}"
        game = db.query(Game).filter(Game.slug == slug).first()
        if game:
            return game

        summary, detail = _resolve_steam_metadata(app_id)
        if not detail:
            raise HTTPException(status_code=404, detail="Steam app not found")
        summary = summary or {}

        game = Game(
            slug=slug,
            title=detail.get("name") or slug,
            short_description=detail.get("short_description"),
            developer=_first_or_none(detail.get("developers")),
            publisher=_first_or_none(detail.get("publishers")),
            release_date=detail.get("release_date") or summary.get("release_date"),
            genres=detail.get("genres") or summary.get("genres") or [],
            platforms=detail.get("platforms") or summary.get("platforms") or [],
            price=0.0,
            discount_percent=0,
            rating=0.0,
            header_image=detail.get("header_image") or summary.get("header_image"),
            hero_image=(
                detail.get("background")
                or detail.get("header_image")
                or summary.get("background")
                or summary.get("header_image")
            ),
            background_image=detail.get("background") or summary.get("background"),
            screenshots=detail.get("screenshots") or [],
            videos=detail.get("movies") or [],
            system_requirements=detail.get("pc_requirements") or {},
            is_published=False,
        )
        db.add(game)
        db.commit()
        db.refresh(game)
        return game

    raise HTTPException(status_code=400, detail="slug, game_id, or app_id is required")


def _task_to_dict(task: DownloadTask) -> dict[str, Any]:
    return {
        "id": task.id,
        "status": task.status,
        "progress": task.progress,
        "downloaded_bytes": int(task.downloaded_bytes or 0),
        "total_bytes": int(task.total_bytes or 0),
        "network_bps": int(task.network_bps or 0),
        "disk_read_bps": int(task.disk_read_bps or 0),
        "disk_write_bps": int(task.disk_write_bps or 0),
        "updated_at": task.updated_at.isoformat() if task.updated_at else None,
        "game_id": task.game_id,
    }


@router.post("")
def create_download_session_v2(
    payload: DownloadSessionCreateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = _ensure_game(payload, db)
    existing = (
        db.query(DownloadTask)
        .filter(DownloadTask.user_id == current_user.id, DownloadTask.game_id == game.id)
        .first()
    )
    if existing:
        task = existing
    else:
        task = DownloadTask(
            user_id=current_user.id,
            game_id=game.id,
            status="downloading",
            progress=0,
            speed_mbps=0.0,
            eta_minutes=0,
            downloaded_bytes=0,
            total_bytes=0,
            network_bps=0,
            disk_read_bps=0,
            disk_write_bps=0,
            read_bytes=0,
            written_bytes=0,
            remaining_bytes=0,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(task)
        db.commit()
        db.refresh(task)

    session = runtime_v2.create_session(
        user_id=current_user.id,
        download_id=task.id,
        game_id=game.id,
        slug=game.slug,
        channel=(payload.channel or "stable").strip().lower(),
        method=(payload.method or "chunks").strip().lower(),
        version=(payload.version or "latest").strip(),
        install_path=payload.install_path,
        meta={
            "pipeline": [
                "manifest_fetch",
                "plan_build",
                "chunk_transfer",
                "verify",
                "xdelta_optional",
                "finalize",
            ],
            "xdelta_policy": {"enabled": True, "min_file_mb": 64},
        },
    )
    runtime_v2.set_session_stage(session["id"], stage="plan_build", status="queued")
    session = runtime_v2.set_session_stage(session["id"], stage="chunk_transfer", status="downloading")

    return {
        "session": session,
        "task": _task_to_dict(task),
    }


@router.post("/{session_id}/control")
def control_download_session_v2(
    session_id: str,
    payload: DownloadSessionControlIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    session = runtime_v2.control_session(session_id, payload.action)
    if not session:
        raise HTTPException(status_code=404, detail="Download session not found")
    if session.get("user_id") and session["user_id"] != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden")

    task = (
        db.query(DownloadTask)
        .filter(DownloadTask.id == session["download_id"], DownloadTask.user_id == current_user.id)
        .first()
    )
    if task:
        if payload.action == "pause":
            task.status = "paused"
        elif payload.action == "resume":
            task.status = "downloading"
        elif payload.action == "cancel":
            task.status = "cancelled"
        task.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(task)

    return {
        "session": session,
        "task": _task_to_dict(task) if task else None,
    }


@router.get("/{session_id}/state")
def get_download_session_state_v2(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    session = runtime_v2.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Download session not found")
    if session.get("user_id") and session["user_id"] != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden")

    task = (
        db.query(DownloadTask)
        .filter(DownloadTask.id == session["download_id"], DownloadTask.user_id == current_user.id)
        .first()
    )
    if task and task.status == "completed":
        session = runtime_v2.set_session_stage(session_id, stage="finalize", status="completed") or session
    return {
        "session": session,
        "task": _task_to_dict(task) if task else None,
    }
