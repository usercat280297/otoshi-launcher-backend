"""Updates module for auto-update system."""

from .update_manager import FileInfo, UpdateManager, UpdateManifest, UpdateVersion
from .routes import router

__all__ = [
    "UpdateManager",
    "UpdateVersion",
    "UpdateManifest",
    "FileInfo",
    "router",
]
