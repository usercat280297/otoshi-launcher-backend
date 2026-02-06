"""
Game Properties API - Verify, Uninstall, Move, Cloud Sync
"""

from fastapi import APIRouter, HTTPException, Depends, Body
from pydantic import BaseModel
from typing import Optional, List
import os
import shutil
import hashlib
from pathlib import Path

from ..routes.deps import get_current_user

router = APIRouter()


class GameInstallInfo(BaseModel):
    installed: bool
    install_path: Optional[str] = None
    size_bytes: Optional[int] = None
    version: Optional[str] = None
    last_played: Optional[str] = None


class VerifyResult(BaseModel):
    success: bool
    total_files: int
    verified_files: int
    corrupted_files: int
    missing_files: int


class MoveRequest(BaseModel):
    source_path: str
    dest_path: str


class CloudSyncResult(BaseModel):
    success: bool
    files_uploaded: int
    files_downloaded: int
    conflicts: int


def get_folder_size(path: str) -> int:
    """Calculate total size of a folder in bytes"""
    total = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.isfile(fp):
                    total += os.path.getsize(fp)
    except Exception:
        pass
    return total


def count_files(path: str) -> int:
    """Count total files in a folder"""
    count = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            count += len(filenames)
    except Exception:
        pass
    return count


def verify_files_in_folder(path: str) -> tuple:
    """Verify all files in a folder, return (total, verified, corrupted)"""
    total = 0
    verified = 0
    corrupted = 0

    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total += 1
                try:
                    # Basic verification - check file is readable
                    if os.path.isfile(fp) and os.access(fp, os.R_OK):
                        # Could add hash verification here with a manifest
                        verified += 1
                    else:
                        corrupted += 1
                except Exception:
                    corrupted += 1
    except Exception:
        pass

    return total, verified, corrupted


def is_valid_game_folder(path: str) -> bool:
    """Check if path looks like a valid game folder"""
    indicators = [
        "steam_appid.txt",
        "steam_api.dll",
        "steam_api64.dll",
        "Binaries",
        "Engine",
        "UnityCrashHandler64.exe",
        "UnityPlayer.dll",
    ]

    for indicator in indicators:
        if os.path.exists(os.path.join(path, indicator)):
            return True

    # Check for executable files
    try:
        for f in os.listdir(path):
            if f.endswith(".exe"):
                return True
    except Exception:
        pass

    return False


@router.get("/{app_id}/info", response_model=GameInstallInfo)
def get_install_info(app_id: str):
    """Get installation information for a game"""
    # Try to find the game in common locations
    # This is a simplified version - real implementation would check Steam library folders

    common_paths = [
        f"C:\\Program Files (x86)\\Steam\\steamapps\\common",
        f"C:\\Program Files\\Steam\\steamapps\\common",
        f"D:\\SteamLibrary\\steamapps\\common",
        f"E:\\SteamLibrary\\steamapps\\common",
    ]

    for base_path in common_paths:
        if not os.path.exists(base_path):
            continue

        try:
            for folder in os.listdir(base_path):
                folder_path = os.path.join(base_path, folder)
                appid_file = os.path.join(folder_path, "steam_appid.txt")

                if os.path.exists(appid_file):
                    try:
                        with open(appid_file, "r") as f:
                            if f.read().strip() == app_id:
                                size = get_folder_size(folder_path)
                                return GameInstallInfo(
                                    installed=True,
                                    install_path=folder_path,
                                    size_bytes=size,
                                    version=None,
                                    last_played=None,
                                )
                    except Exception:
                        continue
        except Exception:
            continue

    return GameInstallInfo(installed=False)


@router.post("/{app_id}/verify", response_model=VerifyResult)
def verify_game(app_id: str, install_path: str = Body(..., embed=True)):
    """Verify game file integrity"""
    if not os.path.exists(install_path):
        raise HTTPException(status_code=404, detail="Install path not found")

    if not is_valid_game_folder(install_path):
        raise HTTPException(status_code=400, detail="Invalid game folder")

    total, verified, corrupted = verify_files_in_folder(install_path)

    return VerifyResult(
        success=corrupted == 0,
        total_files=total,
        verified_files=verified,
        corrupted_files=corrupted,
        missing_files=0,  # Would need manifest to check missing
    )


@router.post("/{app_id}/uninstall")
def uninstall_game(
    app_id: str,
    install_path: str = Body(..., embed=True),
    current_user: dict = Depends(get_current_user),
):
    """Uninstall a game by removing its folder"""
    if not os.path.exists(install_path):
        raise HTTPException(status_code=404, detail="Install path not found")

    if not is_valid_game_folder(install_path):
        raise HTTPException(status_code=400, detail="Invalid game folder")

    try:
        shutil.rmtree(install_path)
        return {"success": True, "message": "Game uninstalled successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to uninstall: {str(e)}")


@router.post("/{app_id}/move")
def move_game(
    app_id: str,
    request: MoveRequest,
    current_user: dict = Depends(get_current_user),
):
    """Move game to a new location"""
    if not os.path.exists(request.source_path):
        raise HTTPException(status_code=404, detail="Source path not found")

    if os.path.exists(request.dest_path):
        raise HTTPException(status_code=400, detail="Destination already exists")

    if not is_valid_game_folder(request.source_path):
        raise HTTPException(status_code=400, detail="Invalid game folder")

    try:
        # Try rename first (fast if same drive)
        try:
            os.rename(request.source_path, request.dest_path)
        except OSError:
            # Fall back to copy + delete
            shutil.copytree(request.source_path, request.dest_path)
            shutil.rmtree(request.source_path)

        return {
            "success": True,
            "new_path": request.dest_path,
            "message": "Game moved successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to move: {str(e)}")


@router.post("/{app_id}/cloud-sync", response_model=CloudSyncResult)
def sync_cloud_saves(
    app_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Sync game saves with cloud storage"""
    # In a real implementation, this would:
    # 1. Find local save locations for the game
    # 2. Compare with cloud saves
    # 3. Upload newer local saves
    # 4. Download newer cloud saves
    # 5. Handle conflicts

    # Simulated response
    return CloudSyncResult(
        success=True,
        files_uploaded=0,
        files_downloaded=0,
        conflicts=0,
    )


@router.get("/{app_id}/save-locations")
def get_save_locations(app_id: str):
    """Get known save game locations for a game"""
    # Common save locations
    user_profile = os.environ.get("USERPROFILE", "")

    locations = []

    common_paths = [
        os.path.join(user_profile, "Saved Games"),
        os.path.join(user_profile, "Documents", "My Games"),
        os.path.join(user_profile, "AppData", "Local"),
        os.path.join(user_profile, "AppData", "LocalLow"),
        os.path.join(user_profile, "AppData", "Roaming"),
    ]

    for path in common_paths:
        if os.path.exists(path):
            locations.append(path)

    return {"app_id": app_id, "locations": locations}
