"""
Auto-update endpoints for the launcher.
Serves update information for Tauri's built-in updater and custom update mechanisms.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from .deps import require_admin_access
router = APIRouter(prefix="/updates", tags=["updates"])

# Update configuration - can be overridden via environment variables
_UPDATE_CONFIG_FILE = Path(__file__).resolve().parents[2] / "data" / "update_config.json"

# Current version (should match Cargo.toml / tauri.conf.json)
CURRENT_VERSION = os.getenv("APP_VERSION", "0.1.0")


class UpdateInfo(BaseModel):
    version: str
    notes: str
    pub_date: str
    url: str
    signature: str


class UpdateCheckResponse(BaseModel):
    update_available: bool
    current_version: str
    latest_version: Optional[str] = None
    update_info: Optional[UpdateInfo] = None


def _load_update_config() -> dict:
    """Load update configuration from JSON file."""
    if not _UPDATE_CONFIG_FILE.exists():
        return {}
    try:
        return json.loads(_UPDATE_CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _compare_versions(current: str, latest: str) -> int:
    """
    Compare two version strings.
    Returns: -1 if current < latest, 0 if equal, 1 if current > latest
    """
    def parse_version(v: str) -> Tuple[int, ...]:
        parts = []
        for part in v.replace("-", ".").split("."):
            try:
                parts.append(int(part))
            except ValueError:
                parts.append(0)  # Convert non-numeric to 0
        return tuple(parts)
    
    current_parts = parse_version(current)
    latest_parts = parse_version(latest)
    
    if current_parts < latest_parts:
        return -1
    if current_parts > latest_parts:
        return 1
    return 0


@router.get("/check")
async def check_update(request: Request, current_version: Optional[str] = None) -> UpdateCheckResponse:
    """
    Check if an update is available.
    This endpoint is called by the launcher to check for updates.
    """
    version = current_version or CURRENT_VERSION
    config = _load_update_config()
    
    latest = config.get("latest_version")
    if not latest:
        return UpdateCheckResponse(
            update_available=False,
            current_version=version,
        )
    
    if _compare_versions(version, latest) >= 0:
        return UpdateCheckResponse(
            update_available=False,
            current_version=version,
            latest_version=latest,
        )
    
    # Get platform-specific download URL
    platform = request.headers.get("X-Platform", "windows-x86_64")
    download_urls = config.get("download_urls", {})
    download_url = download_urls.get(platform, download_urls.get("windows-x86_64", ""))
    
    signatures = config.get("signatures", {})
    signature = signatures.get(platform, signatures.get("windows-x86_64", ""))
    
    update_info = UpdateInfo(
        version=latest,
        notes=config.get("release_notes", ""),
        pub_date=config.get("pub_date", datetime.utcnow().isoformat() + "Z"),
        url=download_url,
        signature=signature,
    )
    
    return UpdateCheckResponse(
        update_available=True,
        current_version=version,
        latest_version=latest,
        update_info=update_info,
    )


@router.get("/tauri/{target}/{current_version}")
async def tauri_update_check(target: str, current_version: str):
    """
    Tauri-compatible update endpoint.
    Returns 204 No Content if no update, or JSON with update info.
    Format: https://tauri.app/v1/guides/distribution/updater/
    """
    config = _load_update_config()
    latest = config.get("latest_version")
    
    if not latest or _compare_versions(current_version, latest) >= 0:
        raise HTTPException(status_code=204, detail="No update available")
    
    # Map target to platform
    platform_map = {
        "windows-x86_64": "windows-x86_64",
        "windows-aarch64": "windows-aarch64",
        "linux-x86_64": "linux-x86_64",
        "darwin-x86_64": "darwin-x86_64",
        "darwin-aarch64": "darwin-aarch64",
    }
    platform = platform_map.get(target, "windows-x86_64")
    
    download_urls = config.get("download_urls", {})
    download_url = download_urls.get(platform)
    
    if not download_url:
        raise HTTPException(status_code=204, detail="No update available for this platform")
    
    signatures = config.get("signatures", {})
    signature = signatures.get(platform, "")
    
    return {
        "version": latest,
        "notes": config.get("release_notes", ""),
        "pub_date": config.get("pub_date", datetime.utcnow().isoformat() + "Z"),
        "url": download_url,
        "signature": signature,
    }


@router.get("/manifest/refresh")
async def refresh_manifests(_: object = Depends(require_admin_access)):
    """
    Force refresh of remote manifest cache.
    Call this when you've added new games or versions to HuggingFace.
    """
    try:
        from ..services.remote_manifests import clear_cache, list_remote_manifests
        clear_cache()
        manifests = list_remote_manifests(force_refresh=True)
        return {
            "success": True,
            "message": f"Refreshed {len(manifests)} manifests from remote",
            "manifests": [
                {
                    "game_name": m.game_name,
                    "version": m.version,
                    "folder": m.folder,
                }
                for m in manifests
            ]
        }
    except ImportError:
        return {
            "success": False,
            "message": "Remote manifest module not available",
            "manifests": []
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "manifests": []
        }


@router.get("/config")
async def get_remote_config():
    """
    Get remote configuration for the launcher.
    This allows changing launcher behavior without rebuilding.
    """
    config = _load_update_config()
    return {
        "features": config.get("features", {}),
        "announcements": config.get("announcements", []),
        "maintenance_mode": config.get("maintenance_mode", False),
        "maintenance_message": config.get("maintenance_message", ""),
    }
