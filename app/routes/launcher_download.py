"""
Launcher download routes - Serve launcher installer files
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from pathlib import Path
import os
import hashlib
import re
import zipfile
import json
from datetime import datetime
from typing import Optional

from .deps import require_admin_access

router = APIRouter()

# Configuration
DOWNLOADS_DIR = Path(os.environ.get("LAUNCHER_DOWNLOADS_DIR", "E:/OTOSHI LAUNCHER/dist"))
LAUNCHER_VERSION = "0.1.0"
_artifact_registry_raw = os.environ.get("LAUNCHER_ARTIFACT_REGISTRY_PATH", "").strip()
ARTIFACT_REGISTRY_PATH = (
    Path(_artifact_registry_raw)
    if _artifact_registry_raw
    else (DOWNLOADS_DIR / "artifacts.registry.json")
)


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
    platform: Optional[str] = None
    channel: Optional[str] = None
    published_at: Optional[str] = None


class LauncherArtifactPublishItem(BaseModel):
    kind: str
    filename: str
    version: Optional[str] = None
    size_bytes: Optional[int] = None
    sha256: Optional[str] = None
    download_url: Optional[str] = None
    platform: Optional[str] = None
    channel: Optional[str] = None


class LauncherArtifactPublishIn(BaseModel):
    artifacts: list[LauncherArtifactPublishItem]
    published_at: Optional[str] = None


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


def _infer_platform_from_filename(filename: str, explicit: Optional[str] = None) -> str:
    if explicit and str(explicit).strip():
        return str(explicit).strip().lower()
    lower = filename.lower()
    if lower.endswith(".dmg"):
        return "macos"
    if lower.endswith(".appimage") or lower.endswith(".tar.gz"):
        return "linux"
    return "windows"


def _normalize_download_url(filename: str, download_url: Optional[str]) -> str:
    normalized = str(download_url or "").strip()
    if normalized.startswith("/launcher-download/file/"):
        return normalized
    safe_name = Path(filename).name
    return f"/launcher-download/file/{safe_name}"


def _load_registry_artifacts() -> list[LauncherArtifact]:
    if not ARTIFACT_REGISTRY_PATH.exists():
        return []
    try:
        raw_payload = json.loads(ARTIFACT_REGISTRY_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    raw_items = raw_payload.get("artifacts") if isinstance(raw_payload, dict) else []
    if not isinstance(raw_items, list):
        return []

    items: list[LauncherArtifact] = []
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        filename = Path(str(raw_item.get("filename") or "")).name
        if not filename:
            continue
        try:
            item = LauncherArtifact(
                kind=str(raw_item.get("kind") or "portable"),
                version=str(raw_item.get("version") or _extract_version(filename)),
                filename=filename,
                size_bytes=int(raw_item.get("size_bytes") or 0),
                sha256=str(raw_item.get("sha256") or ""),
                download_url=_normalize_download_url(filename, raw_item.get("download_url")),
                platform=_infer_platform_from_filename(filename, raw_item.get("platform")),
                channel=str(raw_item.get("channel") or "").strip() or None,
                published_at=str(raw_item.get("published_at") or "").strip() or None,
            )
        except Exception:
            continue
        if item.size_bytes <= 0 or not item.sha256:
            path = DOWNLOADS_DIR / filename
            if path.exists():
                item.size_bytes = int(path.stat().st_size)
                item.sha256 = get_file_hash(path)
        if item.size_bytes > 0 and item.sha256:
            items.append(item)
    return items


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
    registry_items = _load_registry_artifacts()
    if registry_items:
        return registry_items

    artifacts: list[LauncherArtifact] = []
    installer = find_installer_file()
    if installer:
        artifact = _to_artifact(installer, "installer")
        artifact.platform = _infer_platform_from_filename(installer.name)
        artifact.channel = "stable"
        artifact.published_at = datetime.utcnow().isoformat()
        artifacts.append(artifact)

    portable = find_portable_file()
    if portable:
        artifact = _to_artifact(portable, "portable")
        artifact.platform = _infer_platform_from_filename(portable.name)
        artifact.channel = "stable"
        artifact.published_at = datetime.utcnow().isoformat()
        artifacts.append(artifact)

    if not artifacts:
        raise HTTPException(status_code=404, detail="No launcher artifacts found")
    return artifacts


@router.post("/artifacts/publish", response_model=list[LauncherArtifact])
def publish_launcher_artifacts(
    payload: LauncherArtifactPublishIn,
    _: object = Depends(require_admin_access),
):
    if not payload.artifacts:
        raise HTTPException(status_code=400, detail="No artifacts provided")

    published_at = payload.published_at or datetime.utcnow().isoformat()
    published: list[LauncherArtifact] = []

    for item in payload.artifacts:
        filename = Path(item.filename).name
        if not filename:
            continue
        file_path = DOWNLOADS_DIR / filename
        if not file_path.exists() and not item.download_url:
            continue

        version = str(item.version or _extract_version(filename))
        size_bytes = int(item.size_bytes or (file_path.stat().st_size if file_path.exists() else 0))
        sha256 = str(item.sha256 or (get_file_hash(file_path) if file_path.exists() else ""))
        if size_bytes <= 0 or not sha256:
            continue

        published.append(
            LauncherArtifact(
                kind=str(item.kind or "portable"),
                version=version,
                filename=filename,
                size_bytes=size_bytes,
                sha256=sha256,
                download_url=_normalize_download_url(filename, item.download_url),
                platform=_infer_platform_from_filename(filename, item.platform),
                channel=str(item.channel or "").strip() or "stable",
                published_at=published_at,
            )
        )

    if not published:
        raise HTTPException(status_code=400, detail="No valid artifacts resolved for publish")

    ARTIFACT_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    serialized = [
        item.model_dump() if hasattr(item, "model_dump") else item.dict()
        for item in published
    ]
    registry_payload = {
        "updated_at": published_at,
        "artifacts": serialized,
    }
    try:
        ARTIFACT_REGISTRY_PATH.write_text(
            json.dumps(registry_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to write artifact registry: {exc}")

    return published


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
