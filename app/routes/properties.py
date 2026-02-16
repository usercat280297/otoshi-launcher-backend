"""
Game Properties API - Steam-level operations for install, verify, move, cloud sync, and launch options.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from ..db import Base, engine, get_db
from ..models import SaveSyncEvent, SaveSyncState, User
from ..routes.deps import get_current_user, get_current_user_optional
from ..services.steam_catalog import get_steam_summary
from ..services.steam_extended import get_steam_dlc

router = APIRouter()

_NAME_CLEAN = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
_SCHEMA_READY = False


def _ensure_properties_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    Base.metadata.create_all(bind=engine)
    _SCHEMA_READY = True


def _retry_on_missing_table(action):
    _ensure_properties_schema()
    try:
        return action()
    except OperationalError as exc:
        if "no such table" not in str(exc).lower():
            raise
        _ensure_properties_schema()
        return action()


class HashMismatchOut(BaseModel):
    path: str
    expected_hash: Optional[str] = None
    actual_hash: Optional[str] = None
    reason: str


class GameInstallInfo(BaseModel):
    installed: bool
    install_path: Optional[str] = None
    install_roots: List[str] = Field(default_factory=list)
    size_bytes: Optional[int] = None
    version: Optional[str] = None
    branch: Optional[str] = None
    build_id: Optional[str] = None
    last_played: Optional[str] = None
    playtime_local_hours: float = 0.0


class VerifyRequest(BaseModel):
    install_path: str
    manifest_version: Optional[str] = None
    max_mismatches: int = Field(default=200, ge=20, le=2000)


class VerifyResult(BaseModel):
    success: bool
    total_files: int
    verified_files: int
    corrupted_files: int
    missing_files: int
    manifest_version: Optional[str] = None
    mismatch_files: List[HashMismatchOut] = Field(default_factory=list)


class MoveRequest(BaseModel):
    source_path: str
    dest_path: str


class MoveResult(BaseModel):
    success: bool
    new_path: str
    progress_token: str
    message: str


class CloudSyncResult(BaseModel):
    success: bool
    files_uploaded: int
    files_downloaded: int
    conflicts: int
    resolution: List[str] = Field(default_factory=list)
    event_id: Optional[str] = None


class LaunchOptionsIn(BaseModel):
    overlay_enabled: Optional[bool] = None
    language: Optional[str] = None
    launch_args: Optional[str] = None
    compatibility_flags: List[str] = Field(default_factory=list)
    privacy_hidden: Optional[bool] = None
    mark_private: Optional[bool] = None
    dlc_overrides: Optional[Dict[str, bool]] = None
    customization: Optional[Dict[str, Any]] = None


class LaunchOptionsOut(BaseModel):
    app_id: str
    user_id: Optional[str] = None
    launch_options: Dict[str, Any] = Field(default_factory=dict)
    updated_at: Optional[str] = None


class SaveLocationsOut(BaseModel):
    app_id: str
    locations: List[str] = Field(default_factory=list)


class DlcStateItemOut(BaseModel):
    app_id: str
    title: str
    installed: bool = False
    enabled: bool = True
    size_bytes: Optional[int] = None
    header_image: Optional[str] = None


def _normalize_name(value: str) -> str:
    return " ".join(_NAME_CLEAN.sub(" ", (value or "").lower()).split())


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _find_steam_libraries() -> list[str]:
    candidates: list[str] = []
    steam_root_candidates = [
        Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "Steam",
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Steam",
    ]
    for root in steam_root_candidates:
        vdf = root / "steamapps" / "libraryfolders.vdf"
        if not vdf.exists():
            continue
        try:
            content = vdf.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for match in re.finditer(r'"path"\s*"([^"]+)"', content):
            raw = match.group(1).replace("\\\\", "\\")
            common = str(Path(raw) / "steamapps" / "common")
            if common not in candidates:
                candidates.append(common)
    return candidates


def _candidate_install_roots() -> list[str]:
    roots: list[str] = []
    env_candidates = [
        os.environ.get("DEFAULT_INSTALL_ROOT"),
        os.environ.get("OTOSHI_INSTALL_ROOT"),
        os.environ.get("GAMES_INSTALL_ROOT"),
    ]
    for value in env_candidates:
        if value and value not in roots:
            roots.append(value)

    program_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")
    program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
    defaults = [
        os.path.join(program_files_x86, "Otoshi Launcher", "otoshiapps", "common"),
        os.path.join(program_files, "Otoshi Launcher", "otoshiapps", "common"),
        "D:\\OtoshiLibrary\\otoshiapps\\common",
        "E:\\OtoshiLibrary\\otoshiapps\\common",
    ]
    for value in defaults + _find_steam_libraries():
        if value not in roots:
            roots.append(value)
    return roots


def _get_folder_size(path: Path) -> int:
    total = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                if file_path.is_file():
                    total += file_path.stat().st_size
    except OSError:
        return total
    return total


def _count_and_verify_readable(path: Path) -> tuple[int, int, int]:
    total = 0
    readable = 0
    corrupted = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                total += 1
                try:
                    if file_path.is_file() and os.access(file_path, os.R_OK):
                        readable += 1
                    else:
                        corrupted += 1
                except OSError:
                    corrupted += 1
    except OSError:
        pass
    return total, readable, corrupted


def _is_valid_game_folder(path: Path) -> bool:
    indicators = [
        "steam_appid.txt",
        "steam_api.dll",
        "steam_api64.dll",
        "Binaries",
        "Engine",
        "UnityPlayer.dll",
    ]
    for indicator in indicators:
        if (path / indicator).exists():
            return True
    try:
        return any(item.suffix.lower() == ".exe" for item in path.iterdir() if item.is_file())
    except OSError:
        return False


def _find_install_matches(app_id: str) -> list[Path]:
    matches: list[Path] = []
    for root in _candidate_install_roots():
        base = Path(root)
        if not base.exists():
            continue
        try:
            for entry in base.iterdir():
                if not entry.is_dir():
                    continue
                appid_file = entry / "steam_appid.txt"
                if not appid_file.exists():
                    continue
                try:
                    if appid_file.read_text(encoding="utf-8", errors="ignore").strip() == str(app_id):
                        matches.append(entry)
                except OSError:
                    continue
        except OSError:
            continue
    return matches


def _chunk_root_dir() -> Path:
    env_root = os.getenv("CHUNK_MANIFEST_DIR")
    if env_root:
        return Path(env_root)
    return Path(__file__).resolve().parents[2] / "auto_chunk_check_update"


def _chunk_map_file() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "chunk_manifest_map.json"


def _load_chunk_manifest_payload(app_id: str, manifest_version: Optional[str] = None) -> tuple[Optional[str], Optional[dict]]:
    map_file = _chunk_map_file()
    if not map_file.exists():
        return None, None
    try:
        payload = json.loads(map_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, None
    steam_map = payload.get("steam_app_id") if isinstance(payload, dict) else None
    if not isinstance(steam_map, dict):
        return None, None
    entry = steam_map.get(str(app_id))
    if not isinstance(entry, dict):
        return None, None

    folder = str(entry.get("folder") or entry.get("game_name") or "").strip()
    if not folder:
        return None, None

    root_dir = _chunk_root_dir()
    manifest_dir = root_dir / folder
    if not manifest_dir.exists():
        return None, None

    candidates: list[Path] = []
    if manifest_version:
        candidates.extend(
            [
                manifest_dir / f"manifest_{manifest_version}.json",
                manifest_dir / f"manifest_{manifest_version.strip('v')}.json",
            ]
        )
    for path in sorted(manifest_dir.glob("manifest*.json"), reverse=True):
        if path not in candidates:
            candidates.append(path)

    for path in candidates:
        if not path.exists():
            continue
        try:
            manifest_payload = json.loads(path.read_text(encoding="utf-8"))
            return str(manifest_payload.get("version") or manifest_version or ""), manifest_payload
        except (OSError, json.JSONDecodeError):
            continue
    return None, None


def _read_checksums_manifest(install_path: Path) -> dict[str, str]:
    checksums = install_path / "checksums.sha256"
    if not checksums.exists():
        return {}
    parsed: dict[str, str] = {}
    try:
        for line in checksums.read_text(encoding="utf-8", errors="ignore").splitlines():
            parts = line.strip().split()
            if len(parts) < 2:
                continue
            digest = parts[0].strip().lower()
            rel_path = " ".join(parts[1:]).replace("\\", "/").lstrip("./")
            if digest and rel_path:
                parsed[rel_path] = digest
    except OSError:
        return {}
    return parsed


def _verify_expected_hashes(
    install_path: Path,
    expected_hashes: dict[str, str],
    max_mismatches: int,
) -> list[HashMismatchOut]:
    mismatches: list[HashMismatchOut] = []
    for rel_path, expected in expected_hashes.items():
        file_path = install_path / rel_path
        if not file_path.exists():
            mismatches.append(
                HashMismatchOut(
                    path=rel_path,
                    expected_hash=expected,
                    actual_hash=None,
                    reason="missing",
                )
            )
        elif file_path.is_file():
            try:
                actual = _sha256_file(file_path).lower()
            except OSError:
                mismatches.append(
                    HashMismatchOut(
                        path=rel_path,
                        expected_hash=expected,
                        actual_hash=None,
                        reason="unreadable",
                    )
                )
            else:
                if actual != expected.lower():
                    mismatches.append(
                        HashMismatchOut(
                            path=rel_path,
                            expected_hash=expected,
                            actual_hash=actual,
                            reason="hash_mismatch",
                        )
                    )
        if len(mismatches) >= max_mismatches:
            break
    return mismatches


def _resolve_save_locations(app_id: str) -> list[Path]:
    user_profile = Path(os.environ.get("USERPROFILE", ""))
    if not user_profile.exists():
        return []

    summary = get_steam_summary(app_id) or {}
    game_name = str(summary.get("name") or app_id)
    normalized_game = _normalize_name(game_name).replace(" ", "")
    normalized_app = _normalize_name(app_id).replace(" ", "")
    candidates = [
        user_profile / "Saved Games",
        user_profile / "Documents" / "My Games",
        user_profile / "AppData" / "Local",
        user_profile / "AppData" / "LocalLow",
        user_profile / "AppData" / "Roaming",
    ]
    resolved: list[Path] = []
    seen: set[str] = set()
    for base in candidates:
        if not base.exists():
            continue
        direct_variants = [
            base / app_id,
            base / game_name,
            base / normalized_game,
        ]
        for candidate in direct_variants:
            if candidate.exists() and str(candidate) not in seen:
                seen.add(str(candidate))
                resolved.append(candidate)

        try:
            for child in base.iterdir():
                if not child.is_dir():
                    continue
                normalized_child = _normalize_name(child.name).replace(" ", "")
                if (
                    normalized_app and normalized_app in normalized_child
                ) or (normalized_game and normalized_game in normalized_child):
                    key = str(child)
                    if key not in seen:
                        seen.add(key)
                        resolved.append(child)
        except OSError:
            continue

    return resolved


def _build_local_save_snapshot(locations: list[Path]) -> dict[str, dict]:
    snapshot: dict[str, dict] = {}
    for location in locations:
        if not location.exists():
            continue
        for dirpath, _, filenames in os.walk(location):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                if not file_path.is_file():
                    continue
                rel = f"{location.name}/{file_path.relative_to(location).as_posix()}"
                try:
                    stat = file_path.stat()
                    snapshot[rel] = {
                        "size": int(stat.st_size),
                        "mtime": float(stat.st_mtime),
                        "hash": _sha256_file(file_path),
                    }
                except OSError:
                    continue
    return snapshot


def _resolve_effective_user_id(current_user: Optional[User]) -> Optional[str]:
    if current_user:
        return str(current_user.id)
    return None


@router.get("/{app_id}/info", response_model=GameInstallInfo)
def get_install_info(app_id: str):
    matches = _find_install_matches(app_id)
    roots = _candidate_install_roots()

    if not matches:
        return GameInstallInfo(installed=False, install_roots=roots)

    selected = matches[0]
    size = _get_folder_size(selected)
    build_id = None
    build_file = selected / "build_id.txt"
    if build_file.exists():
        try:
            build_id = build_file.read_text(encoding="utf-8", errors="ignore").strip() or None
        except OSError:
            build_id = None

    return GameInstallInfo(
        installed=True,
        install_path=str(selected),
        install_roots=roots,
        size_bytes=size,
        version=None,
        branch="stable",
        build_id=build_id,
        last_played=None,
        playtime_local_hours=0.0,
    )


@router.post("/{app_id}/verify", response_model=VerifyResult)
def verify_game(app_id: str, payload: VerifyRequest):
    install_path = Path(payload.install_path)
    if not install_path.exists():
        raise HTTPException(status_code=404, detail="Install path not found")
    if not _is_valid_game_folder(install_path):
        raise HTTPException(status_code=400, detail="Invalid game folder")

    total, readable, corrupted = _count_and_verify_readable(install_path)
    mismatches: list[HashMismatchOut] = []

    expected_hashes = _read_checksums_manifest(install_path)
    if expected_hashes:
        mismatches = _verify_expected_hashes(install_path, expected_hashes, payload.max_mismatches)
    else:
        resolved_version, manifest_payload = _load_chunk_manifest_payload(app_id, payload.manifest_version)
        if manifest_payload:
            chunk_hashes: dict[str, str] = {}
            for chunk in manifest_payload.get("chunks") or []:
                if not isinstance(chunk, dict):
                    continue
                path = str(chunk.get("path") or chunk.get("filename") or "").replace("\\", "/").lstrip("/")
                digest = str(chunk.get("hash") or "").strip().lower()
                if path and digest:
                    chunk_hashes[path] = digest
            if chunk_hashes:
                mismatches = _verify_expected_hashes(install_path, chunk_hashes, payload.max_mismatches)
            if resolved_version and not payload.manifest_version:
                payload.manifest_version = resolved_version

    missing_files = sum(1 for entry in mismatches if entry.reason == "missing")
    corrupted_files = corrupted + sum(1 for entry in mismatches if entry.reason in {"hash_mismatch", "unreadable"})
    success = corrupted_files == 0 and missing_files == 0

    return VerifyResult(
        success=success,
        total_files=total,
        verified_files=max(0, readable - len(mismatches)),
        corrupted_files=corrupted_files,
        missing_files=missing_files,
        manifest_version=payload.manifest_version,
        mismatch_files=mismatches,
    )


@router.post("/{app_id}/uninstall")
def uninstall_game(
    app_id: str,
    install_path: str = Body(..., embed=True),
    current_user: User = Depends(get_current_user),
):
    path = Path(install_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Install path not found")
    if not _is_valid_game_folder(path):
        raise HTTPException(status_code=400, detail="Invalid game folder")
    try:
        shutil.rmtree(path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to uninstall: {exc}") from exc
    return {"success": True, "message": "Game uninstalled successfully"}


@router.post("/{app_id}/move", response_model=MoveResult)
def move_game(
    app_id: str,
    request: MoveRequest,
    current_user: User = Depends(get_current_user),
):
    source = Path(request.source_path)
    dest = Path(request.dest_path)
    if not source.exists():
        raise HTTPException(status_code=404, detail="Source path not found")
    if dest.exists():
        raise HTTPException(status_code=400, detail="Destination already exists")
    if not _is_valid_game_folder(source):
        raise HTTPException(status_code=400, detail="Invalid game folder")

    progress_token = f"move-{app_id}-{uuid.uuid4().hex[:12]}"
    try:
        try:
            os.rename(source, dest)
        except OSError:
            shutil.copytree(source, dest)
            shutil.rmtree(source)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to move: {exc}") from exc

    return MoveResult(
        success=True,
        new_path=str(dest),
        progress_token=progress_token,
        message="Game moved successfully",
    )


@router.post("/{app_id}/cloud-sync", response_model=CloudSyncResult)
def sync_cloud_saves(
    app_id: str,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    def _run():
        user_id = _resolve_effective_user_id(current_user)
        locations = _resolve_save_locations(app_id)
        local_snapshot = _build_local_save_snapshot(locations)

        state = (
            db.query(SaveSyncState)
            .filter(SaveSyncState.user_id == user_id, SaveSyncState.app_id == str(app_id))
            .first()
        )
        if not state:
            state = SaveSyncState(
                user_id=user_id,
                app_id=str(app_id),
                version_vector={"desktop": 1},
                checksum_manifest={},
                device_state={},
                launch_options={},
            )
            db.add(state)
            db.flush()

        remote_snapshot = state.checksum_manifest if isinstance(state.checksum_manifest, dict) else {}
        uploads = 0
        downloads = 0
        conflicts = 0
        resolution: list[str] = []

        merged = dict(remote_snapshot)
        for path, local_meta in local_snapshot.items():
            remote_meta = remote_snapshot.get(path)
            if remote_meta is None:
                uploads += 1
                merged[path] = local_meta
                resolution.append(f"upload:new:{path}")
                continue
            local_hash = str(local_meta.get("hash") or "")
            remote_hash = str((remote_meta or {}).get("hash") or "")
            if local_hash == remote_hash:
                continue
            local_mtime = float(local_meta.get("mtime") or 0)
            remote_mtime = float((remote_meta or {}).get("mtime") or 0)
            if local_mtime >= remote_mtime:
                uploads += 1
                merged[path] = local_meta
                resolution.append(f"upload:newer_local:{path}")
            else:
                downloads += 1
                resolution.append(f"download:newer_remote:{path}")

        for path in remote_snapshot.keys():
            if path not in local_snapshot:
                downloads += 1
                resolution.append(f"download:missing_local:{path}")

        if uploads > 0 and downloads > 0:
            conflicts = min(uploads, downloads)

        state.checksum_manifest = merged
        vector = state.version_vector if isinstance(state.version_vector, dict) else {}
        vector["desktop"] = int(vector.get("desktop") or 0) + 1
        state.version_vector = vector
        state.device_state = {
            "last_device": "desktop",
            "locations": [str(item) for item in locations],
            "updated_at": datetime.utcnow().isoformat(),
        }
        state.last_sync_at = datetime.utcnow()

        event = SaveSyncEvent(
            user_id=user_id,
            app_id=str(app_id),
            event_type="sync_apply",
            payload={
                "uploads": uploads,
                "downloads": downloads,
                "conflicts": conflicts,
                "resolution": resolution[:100],
            },
        )
        db.add(event)
        db.commit()
        db.refresh(event)

        return CloudSyncResult(
            success=True,
            files_uploaded=uploads,
            files_downloaded=downloads,
            conflicts=conflicts,
            resolution=resolution[:100],
            event_id=event.id,
        )

    return _retry_on_missing_table(_run)


@router.get("/{app_id}/save-locations", response_model=SaveLocationsOut)
def get_save_locations(app_id: str):
    locations = _resolve_save_locations(app_id)
    return SaveLocationsOut(app_id=app_id, locations=[str(item) for item in locations])


@router.get("/{app_id}/launch-options", response_model=LaunchOptionsOut)
def get_launch_options(
    app_id: str,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    def _run():
        user_id = _resolve_effective_user_id(current_user)
        state = (
            db.query(SaveSyncState)
            .filter(SaveSyncState.user_id == user_id, SaveSyncState.app_id == str(app_id))
            .first()
        )
        options = state.launch_options if state and isinstance(state.launch_options, dict) else {}
        return LaunchOptionsOut(
            app_id=str(app_id),
            user_id=user_id,
            launch_options=options,
            updated_at=state.updated_at.isoformat() if state and state.updated_at else None,
        )

    return _retry_on_missing_table(_run)


@router.post("/{app_id}/launch-options", response_model=LaunchOptionsOut)
def set_launch_options(
    app_id: str,
    payload: LaunchOptionsIn,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    def _run():
        user_id = _resolve_effective_user_id(current_user)
        state = (
            db.query(SaveSyncState)
            .filter(SaveSyncState.user_id == user_id, SaveSyncState.app_id == str(app_id))
            .first()
        )
        if not state:
            state = SaveSyncState(
                user_id=user_id,
                app_id=str(app_id),
                version_vector={"desktop": 1},
                checksum_manifest={},
                device_state={},
                launch_options={},
            )
            db.add(state)
            db.flush()

        existing = state.launch_options if isinstance(state.launch_options, dict) else {}
        updates = payload.model_dump(exclude_none=True)
        existing.update(updates)
        state.launch_options = existing
        state.updated_at = datetime.utcnow()

        event = SaveSyncEvent(
            user_id=user_id,
            app_id=str(app_id),
            event_type="launch_options_set",
            payload={"updates": updates},
        )
        db.add(event)
        db.commit()

        return LaunchOptionsOut(
            app_id=str(app_id),
            user_id=user_id,
            launch_options=existing,
            updated_at=state.updated_at.isoformat() if state.updated_at else None,
        )

    return _retry_on_missing_table(_run)


@router.get("/{app_id}/dlc", response_model=list[DlcStateItemOut])
def get_dlc_state(app_id: str):
    items = get_steam_dlc(app_id) or []
    normalized: list[DlcStateItemOut] = []
    for item in items:
        normalized.append(
            DlcStateItemOut(
                app_id=str(item.get("app_id") or ""),
                title=str(item.get("name") or "Unknown DLC"),
                installed=bool(item.get("installed", False)),
                enabled=bool(item.get("enabled", True)),
                size_bytes=item.get("size_bytes"),
                header_image=item.get("header_image"),
            )
        )
    return normalized
