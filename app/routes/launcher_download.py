"""
Launcher download routes - Serve launcher installer files
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from pathlib import Path
import os
import hashlib
import re
import zipfile

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


class LauncherArtifact(BaseModel):
    kind: str
    version: str
    filename: str
    size_bytes: int
    sha256: str
    download_url: str


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


def _extract_version(name: str) -> str:
    match = re.search(r"v(\d+(?:\.\d+)*)", name, re.IGNORECASE)
    if match:
        return match.group(1)
    return LAUNCHER_VERSION


def _zip_folder(source_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for file_path in source_dir.rglob("*"):
            if not file_path.is_file():
                continue
            arcname = file_path.relative_to(source_dir.parent)
            archive.write(file_path, arcname.as_posix())


def find_portable_file() -> Path | None:
    if not DOWNLOADS_DIR.exists():
        return None

    zipped = sorted(
        DOWNLOADS_DIR.glob("OtoshiLauncher-Portable-*.zip"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if zipped:
        return zipped[0]

    folders = sorted(
        [item for item in DOWNLOADS_DIR.glob("OtoshiLauncher-Portable-*") if item.is_dir()],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if not folders:
        return None

    latest_folder = folders[0]
    zip_path = DOWNLOADS_DIR / f"{latest_folder.name}.zip"
    try:
        folder_mtime = latest_folder.stat().st_mtime
        zip_mtime = zip_path.stat().st_mtime if zip_path.exists() else 0
        if (not zip_path.exists()) or zip_mtime < folder_mtime:
            _zip_folder(latest_folder, zip_path)
        return zip_path
    except Exception:
        return None


def _to_artifact(path: Path, kind: str) -> LauncherArtifact:
    return LauncherArtifact(
        kind=kind,
        version=_extract_version(path.name),
        filename=path.name,
        size_bytes=path.stat().st_size,
        sha256=get_file_hash(path),
        download_url=f"/launcher-download/file/{path.name}",
    )


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


@router.get("/artifacts", response_model=list[LauncherArtifact])
def get_launcher_artifacts():
    artifacts: list[LauncherArtifact] = []
    installer = find_installer_file()
    if installer:
        artifacts.append(_to_artifact(installer, "installer"))

    portable = find_portable_file()
    if portable:
        artifacts.append(_to_artifact(portable, "portable"))

    if not artifacts:
        raise HTTPException(status_code=404, detail="No launcher artifacts found")
    return artifacts


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
