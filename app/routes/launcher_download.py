"""
Launcher download routes - Serve launcher installer files
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from pathlib import Path
import os
import hashlib

router = APIRouter()

# Configuration
DOWNLOADS_DIR = Path(os.environ.get("LAUNCHER_DOWNLOADS_DIR", "E:/OTOSHI LAUNCHER/dist"))
LAUNCHER_VERSION = "0.1.0"


class LauncherInfo(BaseModel):
    version: str
    filename: str
    size_bytes: int
    sha256: str
    download_url: str


class DownloadStats(BaseModel):
    total_downloads: int
    version: str
    platforms: dict


def get_file_hash(filepath: Path) -> str:
    """Calculate SHA256 hash of file"""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def find_installer_file() -> Path | None:
    """Find the latest installer file"""
    if not DOWNLOADS_DIR.exists():
        return None

    # Look for NSIS installer first
    for pattern in ["*Setup*.exe", "*Installer*.exe", "*.msi", "Otoshi*.exe"]:
        files = list(DOWNLOADS_DIR.glob(pattern))
        if files:
            # Return the newest file
            return max(files, key=lambda f: f.stat().st_mtime)

    # Fallback to any exe
    exes = list(DOWNLOADS_DIR.glob("*.exe"))
    if exes:
        return max(exes, key=lambda f: f.stat().st_mtime)

    return None


@router.get("/info", response_model=LauncherInfo)
def get_launcher_info():
    """Get information about the latest launcher version"""
    installer = find_installer_file()

    if not installer:
        raise HTTPException(status_code=404, detail="Installer not found")

    file_hash = get_file_hash(installer)

    return LauncherInfo(
        version=LAUNCHER_VERSION,
        filename=installer.name,
        size_bytes=installer.stat().st_size,
        sha256=file_hash,
        download_url=f"/launcher-download/file/{installer.name}",
    )


@router.get("/file/{filename}")
def download_launcher(filename: str):
    """Download the launcher installer"""
    # Sanitize filename
    safe_filename = Path(filename).name
    filepath = DOWNLOADS_DIR / safe_filename

    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")

    # Security check - ensure file is in downloads dir
    try:
        filepath.resolve().relative_to(DOWNLOADS_DIR.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    return FileResponse(
        path=filepath,
        filename=safe_filename,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={safe_filename}",
        },
    )


@router.get("/check-update")
def check_update(current_version: str = "0.0.0"):
    """Check if a newer version is available"""
    from packaging import version

    try:
        current = version.parse(current_version)
        latest = version.parse(LAUNCHER_VERSION)

        update_available = latest > current

        if update_available:
            installer = find_installer_file()
            if installer:
                return {
                    "update_available": True,
                    "current_version": current_version,
                    "latest_version": LAUNCHER_VERSION,
                    "download_url": f"/launcher-download/file/{installer.name}",
                    "size_bytes": installer.stat().st_size,
                }

        return {
            "update_available": False,
            "current_version": current_version,
            "latest_version": LAUNCHER_VERSION,
        }
    except Exception:
        return {
            "update_available": False,
            "current_version": current_version,
            "latest_version": LAUNCHER_VERSION,
            "error": "Version check failed",
        }


@router.get("/stats", response_model=DownloadStats)
def get_download_stats():
    """Get download statistics"""
    # In production, this would query a database
    return DownloadStats(
        total_downloads=42000,
        version=LAUNCHER_VERSION,
        platforms={
            "windows": 38500,
            "macos": 2800,
            "linux": 700,
        },
    )


@router.get("/changelog")
def get_changelog():
    """Get changelog for the launcher"""
    return {
        "version": LAUNCHER_VERSION,
        "date": "2026-02-02",
        "changes": [
            "🎮 Added Steam Vault integration with DLC, Achievements, News tabs",
            "⚙️ New Properties tab with Verify, Move, Cloud Sync, Uninstall",
            "🎨 Improved UI with hover effects and animations",
            "🔧 Enhanced game file verification",
            "☁️ Cloud save synchronization",
            "🚀 Performance improvements",
            "🐛 Bug fixes and stability improvements",
        ],
        "previous_versions": [
            {
                "version": "0.0.9",
                "date": "2026-01-15",
                "changes": [
                    "Initial release",
                    "Basic game library",
                    "Download management",
                ],
            },
        ],
    }
