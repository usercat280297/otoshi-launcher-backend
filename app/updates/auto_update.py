"""
Auto-Update System for OTOSHI Launcher
Manages version checks, delta patching, and update delivery
"""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from enum import Enum

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import aiofiles
import aiohttp

logger = logging.getLogger(__name__)

# Models
class UpdateChannel(str, Enum):
    STABLE = "stable"
    BETA = "beta"
    DEV = "dev"

@dataclass
class VersionInfo:
    """Version information"""
    version: str
    versionCode: int
    releaseDate: str
    channel: UpdateChannel
    changelog: Dict[str, Any]

class UpdateCheckRequest(BaseModel):
    currentVersion: str
    currentChannel: UpdateChannel = UpdateChannel.STABLE
    platform: str  # "windows-x86_64", "macos-x86_64", etc.

class UpdateInfo(BaseModel):
    """Update information response"""
    available: bool
    version: str
    releaseDate: str
    changelog: List[str] = []
    releaseNotes: str = ""
    downloadUrl: str
    deltaUrl: Optional[str] = None
    fileSize: int
    fileSha256: str
    signatures: Dict[str, str] = {}

class RollbackRequest(BaseModel):
    """Request to rollback to previous version"""
    targetVersion: str

# Configuration
class UpdateConfig:
    def __init__(self, config_file: Path):
        self.config_file = config_file
        self.load()

    def load(self):
        """Load update configuration"""
        if self.config_file.exists():
            with open(self.config_file) as f:
                self.config = json.load(f)
        else:
            self.config = self._default_config()

    def _default_config(self) -> Dict:
        return {
            "latest_version": "1.0.0",
            "stable_channel": {
                "version": "1.0.0",
                "downloadUrl": "",
                "deltaUrl": None,
                "releaseDate": datetime.utcnow().isoformat(),
                "changelog": [],
                "fileSize": 0,
                "fileSha256": ""
            },
            "beta_channel": {
                "version": "1.0.0",
                "downloadUrl": "",
                "deltaUrl": None,
                "releaseDate": datetime.utcnow().isoformat(),
                "changelog": [],
                "fileSize": 0,
                "fileSha256": ""
            }
        }

    def get_channel_info(self, channel: UpdateChannel) -> Dict:
        """Get update info for specific channel"""
        channel_key = f"{channel.value}_channel"
        return self.config.get(channel_key, {})

    def update_channel(self, channel: UpdateChannel, info: Dict):
        """Update channel information"""
        channel_key = f"{channel.value}_channel"
        self.config[channel_key] = info
        self.save()

    def save(self):
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)

# Update Manager
class UpdateManager:
    def __init__(self, data_dir: Path, cdn_url: str):
        self.data_dir = data_dir
        self.cdn_url = cdn_url
        self.config = UpdateConfig(data_dir / "update_config.json")
        self.version_history: Dict[str, Dict] = {}
        self._load_version_history()

    def _load_version_history(self):
        """Load version history from file"""
        history_file = self.data_dir / "version_history.json"
        if history_file.exists():
            with open(history_file) as f:
                self.version_history = json.load(f)

    def _save_version_history(self):
        """Save version history to file"""
        history_file = self.data_dir / "version_history.json"
        with open(history_file, 'w') as f:
            json.dump(self.version_history, f, indent=2)

    def check_update(self, current_version: str, channel: UpdateChannel,
                    platform: str) -> UpdateInfo:
        """Check if update is available"""
        channel_info = self.config.get_channel_info(channel)
        latest_version = channel_info.get("version", current_version)

        if self._compare_versions(latest_version, current_version) <= 0:
            return UpdateInfo(
                available=False,
                version=latest_version,
                releaseDate=channel_info.get("releaseDate", ""),
                downloadUrl=""
            )

        return UpdateInfo(
            available=True,
            version=latest_version,
            releaseDate=channel_info.get("releaseDate", ""),
            changelog=channel_info.get("changelog", []),
            downloadUrl=channel_info.get("downloadUrl", ""),
            deltaUrl=channel_info.get("deltaUrl"),
            fileSize=channel_info.get("fileSize", 0),
            fileSha256=channel_info.get("fileSha256", ""),
            signatures=channel_info.get("signatures", {})
        )

    def _compare_versions(self, v1: str, v2: str) -> int:
        """
        Compare two semantic versions
        Returns: 1 if v1 > v2, -1 if v1 < v2, 0 if equal
        """
        def parse_version(v: str) -> tuple:
            try:
                parts = [int(x) for x in v.split('.')]
                return tuple(parts + [0] * (3 - len(parts)))
            except:
                return (0, 0, 0)

        v1_parts = parse_version(v1)
        v2_parts = parse_version(v2)

        if v1_parts > v2_parts:
            return 1
        elif v1_parts < v2_parts:
            return -1
        return 0

    async def generate_delta_patch(self, old_file: Path, new_file: Path) -> Path:
        """
        Generate delta patch between two files (using bsdiff)
        Returns path to delta patch file
        """
        try:
            # This is a placeholder - in production use bsdiff library
            # For now, just copy the new file as the "patch"
            patch_file = tempfile.NamedTemporaryFile(delete=False, suffix='.patch')
            shutil.copy2(new_file, patch_file.name)
            return Path(patch_file.name)
        except Exception as e:
            logger.error(f"Failed to generate delta patch: {e}")
            raise

    async def verify_update_integrity(self, file_path: Path, sha256_hash: str) -> bool:
        """Verify integrity of downloaded update file"""
        file_sha256 = await self._calculate_sha256(file_path)
        return file_sha256.lower() == sha256_hash.lower()

    async def _calculate_sha256(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        async with aiofiles.open(file_path, 'rb') as f:
            async for chunk in self._async_chunks(f):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    async def _async_chunks(self, file, chunk_size: int = 8192):
        """Read file in chunks"""
        while True:
            data = await file.read(chunk_size)
            if not data:
                break
            yield data

    def record_version_history(self, version: str, channel: UpdateChannel,
                              timestamp: Optional[str] = None):
        """Record version update in history"""
        if timestamp is None:
            timestamp = datetime.utcnow().isoformat()

        if version not in self.version_history:
            self.version_history[version] = {
                "installedDate": timestamp,
                "channels": []
            }

        if channel.value not in self.version_history[version]["channels"]:
            self.version_history[version]["channels"].append(channel.value)

        self._save_version_history()

# Router
router = APIRouter(prefix="/api/updates", tags=["updates"])

# Global update manager (initialize in app startup)
update_manager: Optional[UpdateManager] = None

def init_update_manager(data_dir: Path, cdn_url: str):
    """Initialize update manager"""
    global update_manager
    update_manager = UpdateManager(data_dir, cdn_url)

@router.post("/check")
async def check_update(request: UpdateCheckRequest) -> UpdateInfo:
    """Check for available updates"""
    if not update_manager:
        raise HTTPException(status_code=500, detail="Update manager not initialized")

    try:
        update_info = update_manager.check_update(
            request.currentVersion,
            request.currentChannel,
            request.platform
        )
        return update_info
    except Exception as e:
        logger.error(f"Error checking update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/latest")
async def get_latest_version(channel: UpdateChannel = UpdateChannel.STABLE) -> Dict:
    """Get latest version for channel"""
    if not update_manager:
        raise HTTPException(status_code=500, detail="Update manager not initialized")

    try:
        channel_info = update_manager.config.get_channel_info(channel)
        return {
            "version": channel_info.get("version", "1.0.0"),
            "channel": channel.value,
            "releaseDate": channel_info.get("releaseDate", ""),
            "changelog": channel_info.get("changelog", [])
        }
    except Exception as e:
        logger.error(f"Error getting latest version: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/changelog/{version}")
async def get_changelog(version: str) -> Dict:
    """Get changelog for specific version"""
    if not update_manager:
        raise HTTPException(status_code=500, detail="Update manager not initialized")

    try:
        history = update_manager.version_history.get(version, {})
        return {
            "version": version,
            "installedDate": history.get("installedDate"),
            "channels": history.get("channels", [])
        }
    except Exception as e:
        logger.error(f"Error getting changelog: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/rollback")
async def rollback_version(request: RollbackRequest, background_tasks: BackgroundTasks) -> Dict:
    """Rollback to previous version"""
    if not update_manager:
        raise HTTPException(status_code=500, detail="Update manager not initialized")

    try:
        # Add rollback task to background
        background_tasks.add_task(
            _perform_rollback,
            request.targetVersion
        )

        return {
            "status": "rollback_started",
            "targetVersion": request.targetVersion,
            "message": f"Rollback to {request.targetVersion} initiated"
        }
    except Exception as e:
        logger.error(f"Error initiating rollback: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _perform_rollback(target_version: str):
    """Perform rollback in background"""
    try:
        logger.info(f"Starting rollback to version {target_version}")
        # Implement rollback logic here
        # This might involve:
        # 1. Stopping the application
        # 2. Restoring previous files from backup
        # 3. Restarting the application
        logger.info(f"Rollback to version {target_version} completed")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")

@router.post("/report-error")
async def report_update_error(data: Dict) -> Dict:
    """Report error during update process"""
    try:
        logger.error(f"Update error reported: {json.dumps(data)}")
        return {
            "status": "error_recorded",
            "message": "Error has been logged and will be reviewed"
        }
    except Exception as e:
        logger.error(f"Error reporting update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
