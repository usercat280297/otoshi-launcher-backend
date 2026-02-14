from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import json
import os
from pathlib import Path

from ..db import get_db
from ..models import DownloadTask, Game, User
from ..schemas import DownloadTaskOut, DownloadOptionsOut, DownloadPrepareIn
from .deps import get_current_user, get_current_user_optional
from ..services.steam_catalog import get_catalog_page, get_steam_detail, get_steam_summary
from ..services.download_options import (
    build_download_options,
    ensure_install_directory,
    method_available,
)
from typing import Optional
from ..services.settings import get_download_settings, set_download_settings
from ..core.cache import cache_client
from ..utils.auth_validator import AuthValidator, log_download_attempt, check_download_permission
from ..core.config import MANIFEST_REMOTE_ONLY, HF_REPO_ID, HF_REPO_TYPE, HF_REVISION
from ..services.remote_game_data import get_manifest_from_server

router = APIRouter()


_ALLOWED_STATUSES = {
    "queued",
    "downloading",
    "paused",
    "verifying",
    "completed",
    "failed",
    "cancelled",
}

_STATUS_TRANSITIONS = {
    "queued": {"downloading", "cancelled", "failed"},
    "downloading": {"paused", "verifying", "completed", "failed", "cancelled"},
    "paused": {"downloading", "cancelled", "failed"},
    "verifying": {"completed", "failed", "cancelled"},
    "completed": set(),
    "failed": {"downloading", "cancelled"},
    "cancelled": {"downloading"},
}


def _manifest_root_dir() -> Path:
    env_root = os.getenv("CHUNK_MANIFEST_DIR", "").strip()
    if env_root:
        return Path(env_root)
    return Path(__file__).parent.parent.parent / "auto_chunk_check_update"


def _first_or_none(value):
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


def _update_version_override(app_id: str, version: Optional[str]) -> dict:
    slug = f"steam-{app_id}"
    settings = get_download_settings()
    overrides = settings.get("version_overrides")
    overrides = overrides if isinstance(overrides, dict) else {}
    cleaned = overrides.copy()
    if version and version != "latest":
        cleaned[slug] = version
    else:
        cleaned.pop(slug, None)
    cache_client.delete(f"manifest:{slug}")
    cache_client.delete(f"manifest:{slug}:latest")
    if version:
        cache_client.delete(f"manifest:{slug}:{version}")
    return cleaned


def _set_download_status(task: DownloadTask, status: str) -> None:
    normalized = (status or "").strip().lower()
    if normalized not in _ALLOWED_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid download status")
    current = (task.status or "queued").strip().lower()
    if normalized == current:
        return
    allowed_next = _STATUS_TRANSITIONS.get(current, set())
    if normalized not in allowed_next:
        raise HTTPException(
            status_code=409,
            detail=f"Invalid status transition: {current} -> {normalized}",
        )
    task.status = normalized


@router.get("/", response_model=list[DownloadTaskOut])
def list_downloads(
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """List download tasks for the current user. Returns empty list if not authenticated."""
    if current_user is None:
        return []
    return (
        db.query(DownloadTask)
        .filter(DownloadTask.user_id == current_user.id)
        .all()
    )


@router.post("/start/{game_id}", response_model=DownloadTaskOut)
def start_download(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    existing = (
        db.query(DownloadTask)
        .filter(DownloadTask.user_id == current_user.id, DownloadTask.game_id == game_id)
        .first()
    )
    if existing:
        return existing

    task = DownloadTask(
        user_id=current_user.id,
        game_id=game_id,
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
    )
    game.total_downloads = (game.total_downloads or 0) + 1
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


@router.post("/steam/{app_id}", response_model=DownloadTaskOut)
def start_steam_download(
    app_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    summary, detail = _resolve_steam_metadata(app_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Steam app not found")

    summary = summary or {}
    slug = f"steam-{app_id}"
    game = db.query(Game).filter(Game.slug == slug).first()
    if not game:
        developers = detail.get("developers")
        publishers = detail.get("publishers")
        game = Game(
            slug=slug,
            title=detail.get("name") or slug,
            short_description=detail.get("short_description"),
            developer=_first_or_none(developers),
            publisher=_first_or_none(publishers),
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

    existing = (
        db.query(DownloadTask)
        .filter(DownloadTask.user_id == current_user.id, DownloadTask.game_id == game.id)
        .first()
    )
    if existing:
        return existing

    task = DownloadTask(
        user_id=current_user.id,
        game_id=game.id,
        status="downloading",
        progress=0,
        speed_mbps=0.0,
        eta_minutes=12,
        downloaded_bytes=0,
        total_bytes=0,
        network_bps=0,
        disk_read_bps=0,
        disk_write_bps=0,
        read_bytes=0,
        written_bytes=0,
        remaining_bytes=0,
    )
    game.total_downloads = (game.total_downloads or 0) + 1
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


@router.get("/steam/{app_id}/options", response_model=DownloadOptionsOut)
def get_steam_download_options(
    app_id: str,
):
    options = build_download_options(app_id)
    if not options:
        raise HTTPException(status_code=404, detail="Steam app not found")
    return options


@router.post("/steam/{app_id}/prepare", response_model=DownloadOptionsOut)
def prepare_steam_download(
    app_id: str,
    payload: DownloadPrepareIn,
):
    options = build_download_options(app_id, install_root=payload.install_path)
    if not options:
        raise HTTPException(status_code=404, detail="Steam app not found")
    method = payload.method
    if method and not method_available(method, options.get("methods")):
        raise HTTPException(status_code=400, detail="Download method unavailable")
    install_path = ensure_install_directory(
        payload.install_path,
        options["name"],
        payload.create_subfolder,
    )
    options["install_path"] = install_path
    overrides = _update_version_override(app_id, payload.version)
    set_download_settings(
        {
            "install_root": payload.install_path,
            "create_subfolder": payload.create_subfolder,
            "last_method": payload.method,
            "last_version": payload.version,
            "version_overrides": overrides,
        }
    )
    return options


@router.post("/steam/{app_id}/start", response_model=DownloadTaskOut)
def start_steam_download_with_options(
    app_id: str,
    payload: DownloadPrepareIn,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),  # Optional auth
):
    # Check authentication - user should be logged in
    if not current_user:
        # Try to get user from session or fallback
        log_download_attempt("unknown", app_id, False, "No user authenticated")
        raise HTTPException(
            status_code=401, 
            detail="Authentication required. Please login to download games."
        )
    
    if not current_user.id:
        log_download_attempt("unknown", app_id, False, "Invalid user object")
        raise HTTPException(
            status_code=401,
            detail="Invalid user. Please login again."
        )

    # Check download permission (currently allows all authenticated users)
    if not check_download_permission(current_user.id, app_id):
        log_download_attempt(current_user.id, app_id, False, "Permission denied")
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to download this game. Check your library."
        )
    
    try:
        options = build_download_options(app_id, install_root=payload.install_path)
        if not options:
            log_download_attempt(current_user.id, app_id, False, "Game not found")
            raise HTTPException(status_code=404, detail="Steam app not found")
        
        if payload.method and not method_available(payload.method, options.get("methods")):
            log_download_attempt(current_user.id, app_id, False, "Invalid download method")
            raise HTTPException(status_code=400, detail="Download method unavailable")
        
        install_path = ensure_install_directory(
            payload.install_path,
            options["name"],
            payload.create_subfolder,
        )
        overrides = _update_version_override(app_id, payload.version)
        set_download_settings(
            {
                "install_root": payload.install_path,
                "create_subfolder": payload.create_subfolder,
                "last_method": payload.method,
                "last_version": payload.version,
                "last_install_path": install_path,
                "version_overrides": overrides,
            }
        )
        
        result = start_steam_download(app_id, db=db, current_user=current_user)
        log_download_attempt(current_user.id, app_id, True, None)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        log_download_attempt(current_user.id, app_id, False, str(e))
        raise HTTPException(status_code=500, detail="Download failed to start")


@router.post("/{download_id}/progress", response_model=DownloadTaskOut)
def update_progress(
    download_id: str,
    progress: int,
    downloaded_bytes: Optional[int] = None,
    total_bytes: Optional[int] = None,
    network_bps: Optional[int] = None,
    disk_read_bps: Optional[int] = None,
    disk_write_bps: Optional[int] = None,
    read_bytes: Optional[int] = None,
    written_bytes: Optional[int] = None,
    remaining_bytes: Optional[int] = None,
    speed_mbps: Optional[float] = None,
    eta_minutes: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    task = (
        db.query(DownloadTask)
        .filter(DownloadTask.id == download_id, DownloadTask.user_id == current_user.id)
        .first()
    )
    if not task:
        raise HTTPException(status_code=404, detail="Download task not found")

    task.progress = max(0, min(progress, 100))
    if downloaded_bytes is not None:
        task.downloaded_bytes = max(0, int(downloaded_bytes))
    if total_bytes is not None:
        task.total_bytes = max(0, int(total_bytes))
    if network_bps is not None:
        task.network_bps = max(0, int(network_bps))
    if disk_read_bps is not None:
        task.disk_read_bps = max(0, int(disk_read_bps))
    if disk_write_bps is not None:
        task.disk_write_bps = max(0, int(disk_write_bps))
    if read_bytes is not None:
        task.read_bytes = max(0, int(read_bytes))
    if written_bytes is not None:
        task.written_bytes = max(0, int(written_bytes))
    if remaining_bytes is not None:
        task.remaining_bytes = max(0, int(remaining_bytes))
    if speed_mbps is not None:
        task.speed_mbps = max(0.0, float(speed_mbps))
    if eta_minutes is not None:
        task.eta_minutes = max(0, int(eta_minutes))

    if task.progress >= 100:
        task.status = "completed"
        task.eta_minutes = 0
        task.remaining_bytes = 0
        if task.total_bytes and not task.downloaded_bytes:
            task.downloaded_bytes = task.total_bytes
    elif task.status not in {"paused", "cancelled", "failed"}:
        task.status = "downloading"
    task.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(task)
    return task


@router.post("/{download_id}/pause", response_model=DownloadTaskOut)
def pause_download(
    download_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    task = (
        db.query(DownloadTask)
        .filter(DownloadTask.id == download_id, DownloadTask.user_id == current_user.id)
        .first()
    )
    if not task:
        raise HTTPException(status_code=404, detail="Download task not found")

    if task.status in {"completed", "cancelled"}:
        raise HTTPException(status_code=409, detail=f"Cannot pause task in status {task.status}")
    _set_download_status(task, "paused")
    task.speed_mbps = 0.0
    task.network_bps = 0
    task.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(task)
    return task


@router.post("/{download_id}/resume", response_model=DownloadTaskOut)
def resume_download(
    download_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    task = (
        db.query(DownloadTask)
        .filter(DownloadTask.id == download_id, DownloadTask.user_id == current_user.id)
        .first()
    )
    if not task:
        raise HTTPException(status_code=404, detail="Download task not found")

    if task.status == "completed":
        raise HTTPException(status_code=409, detail="Cannot resume a completed download")
    _set_download_status(task, "downloading")
    task.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(task)
    return task


@router.post("/{download_id}/cancel", response_model=DownloadTaskOut)
def cancel_download(
    download_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task = (
        db.query(DownloadTask)
        .filter(DownloadTask.id == download_id, DownloadTask.user_id == current_user.id)
        .first()
    )
    if not task:
        raise HTTPException(status_code=404, detail="Download task not found")
    if task.status == "completed":
        raise HTTPException(status_code=409, detail="Cannot cancel a completed download")

    _set_download_status(task, "cancelled")
    task.speed_mbps = 0.0
    task.network_bps = 0
    task.disk_read_bps = 0
    task.disk_write_bps = 0
    task.eta_minutes = 0
    task.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(task)
    return task


@router.post("/{download_id}/status", response_model=DownloadTaskOut)
def update_download_status(
    download_id: str,
    status: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    task = (
        db.query(DownloadTask)
        .filter(DownloadTask.id == download_id, DownloadTask.user_id == current_user.id)
        .first()
    )
    if not task:
        raise HTTPException(status_code=404, detail="Download task not found")

    normalized = (status or "").strip().lower()
    _set_download_status(task, normalized)
    if normalized == "completed":
        task.progress = 100
        task.eta_minutes = 0
        task.remaining_bytes = 0
    if normalized in {"paused", "cancelled", "failed"}:
        task.speed_mbps = 0.0
        task.network_bps = 0
        task.disk_read_bps = 0
        task.disk_write_bps = 0
    task.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(task)
    return task


@router.get("/strategy/{game_id}/{version}")
def get_download_strategy(game_id: str, version: str):
    """
    Determine download strategy: chunks vs link
    Returns: { status: "success", strategy: "chunks" | "link", download_url?: string, chunks?: [...] }
    """
    try:
        # Try to load game versions from auto_chunk_check_update
        versions_file = _manifest_root_dir() / "game_versions.json"

        versions_data = {}

        # Try loading game_versions.json first
        if versions_file.exists():
            with open(versions_file, 'r', encoding='utf-8') as f:
                versions_data = json.load(f)
        else:
            # Fallback to bypass_categories.json
            bypass_file = Path(__file__).parent.parent / "data" / "bypass_categories.json"
            if bypass_file.exists():
                with open(bypass_file, 'r', encoding='utf-8') as f:
                    bypass_data = json.load(f)
                    # Convert bypass_categories format to versions format
                    if 'games' in bypass_data:
                        for game_id_key, game_info in bypass_data['games'].items():
                            if game_id_key == game_id:
                                # Create a simple versions structure
                                versions_data[game_id] = {
                                    "latest": {
                                        "link": game_info.get('link'),
                                        "download_url": game_info.get('link'),
                                        "chunks": [],
                                        "category": game_info.get('category'),
                                        "name": game_info.get('name')
                                    }
                                }
                                if version == "latest" or not version:
                                    version = "latest"
                                break

        if not versions_data or game_id not in versions_data:
            return {
                "status": "error",
                "message": f"Game {game_id} not found",
                "strategy": "link"
            }

        game_versions = versions_data[game_id]

        # Use provided version or fallback to latest
        if not version or version == "latest":
            version = next(iter(game_versions.keys())) if game_versions else "latest"

        if version not in game_versions:
            # Try latest version as fallback
            if "latest" in game_versions:
                version = "latest"
            else:
                return {
                    "status": "error",
                    "message": f"Version {version} not found",
                    "strategy": "link"
                }

        version_info = game_versions[version]

        # Check if game has chunks (new system)
        has_chunks = version_info.get('chunks') and len(version_info.get('chunks', [])) > 0

        if has_chunks:
            # Use chunk-based download
            chunks = version_info.get('chunks', [])
            return {
                "status": "success",
                "strategy": "chunks",
                "game_id": game_id,
                "version": version,
                "chunks": chunks,
                "download_url": f"https://huggingface.co/datasets/{chunks[0]['repo']}" if chunks else None
            }
        else:
            # Fallback to link-based download
            download_url = version_info.get('link') or version_info.get('download_url')
            return {
                "status": "success",
                "strategy": "link",
                "game_id": game_id,
                "version": version,
                "download_url": download_url
            }

    except json.JSONDecodeError as e:
        return {
            "status": "error",
            "message": f"Invalid game versions JSON: {str(e)}",
            "strategy": "link"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Server error: {str(e)}",
            "strategy": "link"
        }


@router.get("/chunks-manifest/{game_name}/{version}")
def get_chunks_manifest(game_name: str, version: str):
    """
    Get chunks manifest for a game from auto_chunk_check_update
    Returns manifest with all chunks, hashes, sizes, etc.
    """
    try:
        if MANIFEST_REMOTE_ONLY:
            remote_manifest = get_manifest_from_server(game_name)
            if not remote_manifest:
                return {
                    "status": "error",
                    "message": f"Remote manifest not available for {game_name}",
                    "available": False
                }
            payload = dict(remote_manifest)
            payload.setdefault("game_name", payload.get("name") or game_name)
            payload.setdefault("version", version)
            payload.setdefault("available", True)
            payload.setdefault("status", "success")
            return payload

        # Try to find manifest file
        manifest_dir = _manifest_root_dir() / game_name

        # Try exact version match first
        manifest_file = manifest_dir / f"manifest_{game_name} {version}.json"

        if not manifest_file.exists():
            # Try without version
            manifest_file = manifest_dir / f"manifest_{game_name}.json"

        if not manifest_file.exists():
            # List all manifest files in directory
            if manifest_dir.exists():
                manifests = list(manifest_dir.glob("manifest_*.json"))
                if manifests:
                    # Use first manifest found
                    manifest_file = manifests[0]
                else:
                    return {
                        "status": "error",
                        "message": f"No manifest found for {game_name} v{version}",
                        "available": False
                    }
            else:
                return {
                    "status": "error",
                    "message": f"Game folder {game_name} not found",
                    "available": False
                }

        # Load manifest
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest = json.load(f)

        def _encode_hf_path(value: str) -> str:
            from urllib.parse import quote
            cleaned = str(value).replace("\\", "/").lstrip("/")
            if not cleaned:
                return ""
            return "/".join(quote(part, safe="-_.~") for part in cleaned.split("/"))

        def _build_hf_base_url(repo_id: str) -> str:
            repo_type = (HF_REPO_TYPE or "dataset").strip().lower()
            revision = (HF_REVISION or "main").strip()
            if repo_type == "space":
                return f"https://huggingface.co/spaces/{repo_id}/resolve/{revision}"
            if repo_type == "model":
                return f"https://huggingface.co/{repo_id}/resolve/{revision}"
            return f"https://huggingface.co/datasets/{repo_id}/resolve/{revision}"

        # Return manifest with Hugging Face URLs
        hf_repo = manifest.get("hf_repo") or manifest.get("hf_repo_id") or HF_REPO_ID or "MangaVNteam/lua-games"
        hf_base_url = manifest.get("hf_base_url") or _build_hf_base_url(hf_repo)
        manifest_version = str(manifest.get("version") or version or "").strip() or version
        folder_name = manifest_dir.name if manifest_dir.exists() else str(manifest.get("game_name") or game_name).strip()
        hf_game_path = (
            manifest.get("hf_game_path")
            or manifest.get("hf_folder")
            or f"{folder_name}/{manifest_version}".strip("/")
        )

        # Build chunk URLs
        chunks = manifest.get('chunks', [])
        for chunk in chunks:
            if chunk.get("hf_url"):
                continue
            if chunk.get("url"):
                chunk["hf_url"] = chunk.get("url")
                continue
            chunk_filename = chunk.get('path') or chunk.get('filename') or ''
            if not chunk_filename:
                continue
            safe_path = _encode_hf_path(f"{hf_game_path}/{chunk_filename}")
            chunk['hf_url'] = f"{hf_base_url.rstrip('/')}/{safe_path}"

        return {
            "status": "success",
            "available": True,
            "game_name": manifest.get('game_name'),
            "version": manifest.get('version'),
            "total_chunks": manifest.get('total_chunks'),
            "total_size": manifest.get('total_size'),
            "total_original_size": manifest.get('total_original_size'),
            "compression_ratio": manifest.get('compression_ratio'),
            "chunk_size_mb": manifest.get('chunk_size_mb'),
            "hf_repo": hf_repo,
            "hf_base_url": hf_base_url,
            "hf_game_path": hf_game_path,
            "chunks": chunks
        }

    except json.JSONDecodeError as e:
        return {
            "status": "error",
            "message": f"Invalid manifest JSON: {str(e)}",
            "available": False
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Server error: {str(e)}",
            "available": False
        }


@router.get("/test-auth")
def test_auth(current_user: Optional[User] = Depends(get_current_user_optional)):
    """Test endpoint to verify authentication status"""
    if current_user:
        return {
            "authenticated": True,
            "user_id": current_user.id,
            "username": current_user.username if hasattr(current_user, 'username') else "N/A"
        }
    else:
        return {
            "authenticated": False,
            "message": "Not logged in"
        }


@router.post("/start-download/{game_id}")
def start_simple_download(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """Simplified download endpoint - works with or without full auth"""
    # Allow download if user is authenticated at all
    if not current_user:
        return {
            "status": "error",
            "message": "Please login to download games",
            "requires_auth": True
        }

    try:
        # Check if game exists
        game = db.query(Game).filter(Game.id == game_id).first()
        if not game:
            return {
                "status": "error",
                "message": f"Game {game_id} not found",
                "game_id": game_id
            }

        # Create download task
        existing = (
            db.query(DownloadTask)
            .filter(DownloadTask.user_id == current_user.id, DownloadTask.game_id == game_id)
            .first()
        )

        if existing:
            return {
                "status": "exists",
                "message": "Download already in progress",
                "task_id": existing.id,
                "progress": existing.progress
            }

        task = DownloadTask(
            user_id=current_user.id,
            game_id=game_id,
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
        )

        game.total_downloads = (game.total_downloads or 0) + 1
        db.add(task)
        db.commit()
        db.refresh(task)

        return {
            "status": "success",
            "message": "Download started",
            "task_id": task.id,
            "game_id": game_id,
            "game_name": game.title
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to start download: {str(e)}"
        }
