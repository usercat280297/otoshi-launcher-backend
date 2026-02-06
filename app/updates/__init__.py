"""Updates module for auto-update system."""

from app.updates.update_manager import UpdateManager, UpdateVersion, UpdateManifest, FileInfo
from app.updates.routes import router

__all__ = [
    "UpdateManager",
    "UpdateVersion",
    "UpdateManifest",
    "FileInfo",
    "router",
]
