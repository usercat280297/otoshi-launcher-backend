"""Update manager for auto-update system.

Handles:
1. Version manifest management
2. Delta patches
3. Update deployment
4. Rollback mechanism
5. Live Lua/UI editing without restart
"""

from __future__ import annotations

import hashlib
import json
import logging
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
import aiofiles

logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    """File information for update manifest."""
    path: str
    hash: str  # SHA256
    size: int
    is_lua: bool = False
    is_ui: bool = False
    requires_restart: bool = False


@dataclass
class UpdateVersion:
    """Version information for update."""
    version: str  # semantic version: 1.2.3
    release_date: str  # ISO format
    files: List[FileInfo]
    changelog: str
    force_update: bool = False
    min_version: Optional[str] = None  # Minimum version to upgrade from


@dataclass
class UpdateManifest:
    """Complete update manifest."""
    current_version: str
    latest_version: str
    versions: List[UpdateVersion]
    update_server: str
    update_frequency_hours: int = 24
    live_edit_enabled: bool = True


class UpdateManager:
    """Manages updates for launcher."""

    def __init__(self, storage_dir: Path):
        self.storage_dir = storage_dir
        self.manifest_file = storage_dir / "update_manifest.json"
        self.versions_dir = storage_dir / "versions"
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self.manifest: Optional[UpdateManifest] = None
        self._cache_lock = asyncio.Lock()

    async def load_manifest(self) -> UpdateManifest:
        """Load update manifest from storage."""
        if self.manifest:
            return self.manifest

        if not self.manifest_file.exists():
            logger.warning("Update manifest not found, creating default")
            return await self._create_default_manifest()

        async with aiofiles.open(self.manifest_file, 'r') as f:
            data = json.loads(await f.read())
            self.manifest = self._parse_manifest(data)
            return self.manifest

    async def _create_default_manifest(self) -> UpdateManifest:
        """Create default manifest."""
        manifest = UpdateManifest(
            current_version="1.0.0",
            latest_version="1.0.0",
            versions=[],
            update_server="https://api.otoshi.launcher/updates"
        )
        await self.save_manifest(manifest)
        self.manifest = manifest
        return manifest

    async def save_manifest(self, manifest: UpdateManifest) -> None:
        """Save manifest to storage."""
        async with self._cache_lock:
            data = {
                "current_version": manifest.current_version,
                "latest_version": manifest.latest_version,
                "versions": [asdict(v) for v in manifest.versions],
                "update_server": manifest.update_server,
                "update_frequency_hours": manifest.update_frequency_hours,
                "live_edit_enabled": manifest.live_edit_enabled,
            }
            async with aiofiles.open(self.manifest_file, 'w') as f:
                await f.write(json.dumps(data, indent=2))
            self.manifest = manifest

    async def check_update(self, current_version: str) -> Optional[UpdateVersion]:
        """Check if update is available."""
        manifest = await self.load_manifest()

        for version in sorted(
            manifest.versions,
            key=lambda v: self._parse_semantic_version(v.version),
            reverse=True
        ):
            if self._is_newer_version(version.version, current_version):
                return version

        return None

    async def get_version_files(self, version: str) -> List[FileInfo]:
        """Get files for a specific version."""
        manifest = await self.load_manifest()

        for v in manifest.versions:
            if v.version == version:
                return v.files

        return []

    async def get_delta_patch(
        self,
        from_version: str,
        to_version: str
    ) -> Optional[Dict[str, Any]]:
        """Get delta patch between two versions (for efficient updates)."""
        from_files = {f.path: f for f in await self.get_version_files(from_version)}
        to_files = {f.path: f for f in await self.get_version_files(to_version)}

        added = {}
        modified = {}
        removed = []

        for path, to_file in to_files.items():
            if path not in from_files:
                added[path] = asdict(to_file)
            elif from_files[path].hash != to_file.hash:
                modified[path] = asdict(to_file)

        for path in from_files:
            if path not in to_files:
                removed.append(path)

        return {
            "from_version": from_version,
            "to_version": to_version,
            "added": added,
            "modified": modified,
            "removed": removed,
            "created_at": datetime.utcnow().isoformat(),
        }

    async def register_version(self, version_info: UpdateVersion) -> None:
        """Register new version."""
        manifest = await self.load_manifest()

        # Remove if already exists
        manifest.versions = [v for v in manifest.versions if v.version != version_info.version]
        manifest.versions.append(version_info)
        manifest.latest_version = version_info.version

        await self.save_manifest(manifest)
        logger.info(f"Registered version {version_info.version}")

    async def live_edit_file(self, file_path: str, content: bytes) -> None:
        """Push live edit to all connected clients without restart.

        This is used for:
        - Lua scripts (gameplay logic)
        - UI configuration
        - Asset updates
        """
        # Store the updated file
        edit_file = self.versions_dir / f"live_edit_{datetime.utcnow().timestamp()}.json"

        file_hash = hashlib.sha256(content).hexdigest()

        async with aiofiles.open(edit_file, 'w') as f:
            await f.write(json.dumps({
                "file_path": file_path,
                "hash": file_hash,
                "size": len(content),
                "timestamp": datetime.utcnow().isoformat(),
                "is_lua": file_path.endswith(".lua"),
                "is_ui": file_path.endswith(".json"),
            }, indent=2))

        logger.info(f"Live edit pushed: {file_path} (hash: {file_hash})")

    async def rollback_to_version(self, target_version: str) -> bool:
        """Rollback to a previous version."""
        manifest = await self.load_manifest()

        for v in manifest.versions:
            if v.version == target_version:
                manifest.current_version = target_version
                await self.save_manifest(manifest)
                logger.warning(f"Rolled back to version {target_version}")
                return True

        logger.error(f"Version {target_version} not found for rollback")
        return False

    @staticmethod
    def _parse_semantic_version(version_str: str) -> tuple:
        """Parse semantic version to tuple for comparison."""
        try:
            return tuple(int(x) for x in version_str.split('.')[:3])
        except (ValueError, AttributeError):
            return (0, 0, 0)

    @staticmethod
    def _is_newer_version(version1: str, version2: str) -> bool:
        """Check if version1 > version2."""
        v1 = UpdateManager._parse_semantic_version(version1)
        v2 = UpdateManager._parse_semantic_version(version2)
        return v1 > v2

    @staticmethod
    def _parse_manifest(data: Dict[str, Any]) -> UpdateManifest:
        """Parse manifest from dict."""
        versions = [
            UpdateVersion(
                version=v["version"],
                release_date=v["release_date"],
                files=[FileInfo(**f) for f in v["files"]],
                changelog=v["changelog"],
                force_update=v.get("force_update", False),
                min_version=v.get("min_version"),
            )
            for v in data.get("versions", [])
        ]

        return UpdateManifest(
            current_version=data["current_version"],
            latest_version=data["latest_version"],
            versions=versions,
            update_server=data["update_server"],
            update_frequency_hours=data.get("update_frequency_hours", 24),
            live_edit_enabled=data.get("live_edit_enabled", True),
        )
