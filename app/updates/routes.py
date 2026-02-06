"""Update API endpoints for launcher auto-update system."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends
from pydantic import BaseModel

from app.updates.update_manager import UpdateManager, UpdateVersion, FileInfo
from app.routes.deps import require_admin_access

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/updates", tags=["updates"])

# Initialize update manager
STORAGE_DIR = Path("storage/updates")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)
update_manager = UpdateManager(STORAGE_DIR)


class CheckUpdateRequest(BaseModel):
    """Request to check for updates."""
    current_version: str
    launcher_type: str = "tauri"  # tauri, nsis, portable


class CheckUpdateResponse(BaseModel):
    """Response for update check."""
    update_available: bool
    latest_version: Optional[str] = None
    changelog: Optional[str] = None
    force_update: bool = False
    download_url: Optional[str] = None


class FileInfoResponse(BaseModel):
    """Response containing file information."""
    path: str
    hash: str
    size: int
    is_lua: bool = False
    is_ui: bool = False
    requires_restart: bool = False


class VersionInfoResponse(BaseModel):
    """Response containing version information."""
    version: str
    release_date: str
    files: list[FileInfoResponse]
    changelog: str
    force_update: bool = False


class DeltaPatchResponse(BaseModel):
    """Response containing delta patch information."""
    from_version: str
    to_version: str
    added: dict
    modified: dict
    removed: list[str]
    created_at: str


class LiveEditRequest(BaseModel):
    """Request to push live edit."""
    file_path: str
    content_base64: str  # Base64 encoded file content


@router.post("/check", response_model=CheckUpdateResponse)
async def check_update(request: CheckUpdateRequest) -> CheckUpdateResponse:
    """Check if update is available.

    Example:
        POST /api/updates/check
        {
            "current_version": "1.0.0",
            "launcher_type": "tauri"
        }
    """
    try:
        update = await update_manager.check_update(request.current_version)

        if not update:
            return CheckUpdateResponse(update_available=False)

        return CheckUpdateResponse(
            update_available=True,
            latest_version=update.version,
            changelog=update.changelog,
            force_update=update.force_update,
            download_url=f"https://api.otoshi.launcher/files/versions/{update.version}",
        )
    except Exception as e:
        logger.error(f"Error checking update: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/version/{version}", response_model=VersionInfoResponse)
async def get_version_info(version: str) -> VersionInfoResponse:
    """Get information about a specific version.

    Example:
        GET /api/updates/version/1.2.3
    """
    try:
        files = await update_manager.get_version_files(version)
        manifest = await update_manager.load_manifest()

        for v in manifest.versions:
            if v.version == version:
                return VersionInfoResponse(
                    version=v.version,
                    release_date=v.release_date,
                    files=[FileInfoResponse(**asdict(f)) for f in v.files],
                    changelog=v.changelog,
                    force_update=v.force_update,
                )

        raise HTTPException(status_code=404, detail=f"Version {version} not found")
    except Exception as e:
        logger.error(f"Error getting version info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/delta", response_model=DeltaPatchResponse)
async def get_delta_patch(
    from_version: str = Query(...),
    to_version: str = Query(...)
) -> DeltaPatchResponse:
    """Get delta patch between two versions (efficient updates).

    Example:
        GET /api/updates/delta?from_version=1.0.0&to_version=1.0.1

    This allows clients to download only changes instead of full files.
    """
    try:
        patch = await update_manager.get_delta_patch(from_version, to_version)

        if not patch:
            raise HTTPException(
                status_code=404,
                detail=f"No delta available from {from_version} to {to_version}"
            )

        return DeltaPatchResponse(**patch)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting delta patch: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/live-edit")
async def push_live_edit(request: LiveEditRequest, _: object = Depends(require_admin_access)):
    """Push live edit to all connected clients without restart.

    Used for:
    - Lua script updates (gameplay logic)
    - UI configuration changes
    - Asset updates

    Example:
        POST /api/updates/live-edit
        {
            "file_path": "assets/ui/main.json",
            "content_base64": "eyJiYWNrZ3JvdW5kIjogIiMwMDAwMDAifQ=="
        }
    """
    import base64

    try:
        # Decode content
        content = base64.b64decode(request.content_base64)

        # Push to update manager
        await update_manager.live_edit_file(request.file_path, content)

        # In real implementation, broadcast to all connected websocket clients
        # This would integrate with WebSocket handler to notify active launchers
        return {
            "status": "success",
            "message": f"Live edit pushed for {request.file_path}",
            "file_path": request.file_path,
        }
    except Exception as e:
        logger.error(f"Error pushing live edit: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/register-version")
async def register_version(version_info: dict, _: object = Depends(require_admin_access)) -> dict:
    """Register a new version (admin endpoint).

    Example:
        POST /api/updates/register-version
        {
            "version": "1.0.1",
            "release_date": "2024-01-15T10:00:00Z",
            "changelog": "Bug fixes and performance improvements",
            "force_update": false,
            "files": [
                {
                    "path": "launcher.exe",
                    "hash": "abc123...",
                    "size": 50000000,
                    "requires_restart": true
                }
            ]
        }
    """
    try:
        update_version = UpdateVersion(
            version=version_info["version"],
            release_date=version_info["release_date"],
            changelog=version_info["changelog"],
            force_update=version_info.get("force_update", False),
            files=[FileInfo(**f) for f in version_info.get("files", [])],
        )

        await update_manager.register_version(update_version)

        return {
            "status": "success",
            "message": f"Version {update_version.version} registered",
            "version": update_version.version,
        }
    except Exception as e:
        logger.error(f"Error registering version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rollback/{target_version}")
async def rollback_version(target_version: str, _: object = Depends(require_admin_access)) -> dict:
    """Rollback to a previous version (admin endpoint).

    Example:
        POST /api/updates/rollback/1.0.0
    """
    try:
        success = await update_manager.rollback_to_version(target_version)

        if not success:
            raise HTTPException(status_code=404, detail=f"Version {target_version} not found")

        return {
            "status": "success",
            "message": f"Rolled back to version {target_version}",
            "version": target_version,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error rolling back version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


from dataclasses import asdict
