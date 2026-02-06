from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path
from typing import Optional

from ..core.config import HF_REPO_ID, MANIFEST_CACHE_DIR
from ..services.chunk_manifests import (
    get_chunk_manifest_size,
    get_chunk_versions_for_game,
)
from ..services.fixes import get_bypass_option, get_online_fix_options
from ..services.huggingface import huggingface_fetcher
from ..services.settings import get_download_settings
from ..services.steam_catalog import get_steam_detail, get_steam_summary

_SIZE_PATTERN = re.compile(r"(?:Storage|Hard Drive|Disk Space)[^0-9]*([0-9]+(?:\\.[0-9]+)?)\\s*(TB|GB|MB)", re.I)
_VERSIONS_FILE = Path(__file__).resolve().parents[1] / "data" / "steam_versions.json"


def _format_bytes(value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    if value <= 0:
        return "0 MB"
    gb = value / (1024**3)
    if gb >= 1:
        return f"{gb:.1f} GB"
    mb = value / (1024**2)
    return f"{mb:.0f} MB"


def _parse_storage_bytes(texts: list[str]) -> Optional[int]:
    best = None
    for text in texts:
        if not text:
            continue
        for match in _SIZE_PATTERN.finditer(text):
            value = float(match.group(1))
            unit = match.group(2).upper()
            bytes_value = int(value * 1024**2)
            if unit == "GB":
                bytes_value = int(value * 1024**3)
            elif unit == "TB":
                bytes_value = int(value * 1024**4)
            if best is None or bytes_value > best:
                best = bytes_value
    return best


def _estimate_storage_bytes(detail: dict) -> Optional[int]:
    requirements = detail.get("pc_requirements") or {}
    texts = []
    if requirements.get("recommended"):
        texts.append(str(requirements.get("recommended")))
    if requirements.get("minimum"):
        texts.append(str(requirements.get("minimum")))
    return _parse_storage_bytes(texts)


def _manifest_size_bytes(app_id: str) -> Optional[int]:
    cache_dir = MANIFEST_CACHE_DIR.strip()
    if not cache_dir:
        return None
    path = Path(cache_dir) / f"steam-{app_id}.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    size = payload.get("compressed_size") or payload.get("total_size")
    try:
        return int(size)
    except (TypeError, ValueError):
        return None


def _default_install_root() -> str:
    settings = get_download_settings()
    root = settings.get("install_root")
    if root:
        return str(root)
    env_root = os.getenv("DEFAULT_INSTALL_ROOT")
    if env_root:
        return env_root
    return str(Path.home() / "Otoshi Games")


def _normalize_install_root(value: Optional[str]) -> str:
    root = (value or "").strip()
    if not root:
        root = _default_install_root()
    return str(Path(root))


def _disk_usage(path: str) -> tuple[Optional[int], Optional[int]]:
    try:
        target = Path(path)
        if not target.exists():
            target = target.parent if target.parent.exists() else target
        usage = shutil.disk_usage(target)
        return usage.free, usage.total
    except Exception:
        return None, None


def _load_versions(app_id: str) -> list[dict]:
    if not _VERSIONS_FILE.exists():
        return []
    try:
        payload = _VERSIONS_FILE.read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, dict):
        return []
    items = data.get(str(app_id))
    if not isinstance(items, list):
        return []
    versions = []
    for item in items:
        if not isinstance(item, dict):
            continue
        version_id = str(item.get("id") or "").strip()
        if not version_id:
            continue
        versions.append(
            {
                "id": version_id,
                "label": str(item.get("label") or version_id),
                "is_latest": bool(item.get("is_latest")),
                "size_bytes": item.get("size_bytes"),
            }
        )
    return versions


def list_download_versions(app_id: str, release_date: Optional[str]) -> list[dict]:
    versions = _load_versions(app_id)
    if versions:
        versions.sort(key=lambda item: (not item.get("is_latest"), item.get("label")))
        return versions
    label = "Latest"
    if release_date:
        label = f"Latest ({release_date})"
    return [{"id": "latest", "label": label, "is_latest": True, "size_bytes": None}]


def _hf_unavailable_reason(has_manifest: bool) -> Optional[str]:
    if not HF_REPO_ID:
        return "Hugging Face repo not configured"
    if not has_manifest:
        return "No remote manifest (repo private or token missing)"
    return None


def list_download_methods(has_manifest: bool) -> list[dict]:
    hf_enabled = bool(HF_REPO_ID and huggingface_fetcher.enabled() and has_manifest)
    hf_note = None if hf_enabled else _hf_unavailable_reason(has_manifest)
    methods = [
        {
            "id": "hf_chunks",
            "label": "Hugging Face chunks",
            "description": "Resume-friendly chunks with CDN fallback.",
            "recommended": hf_enabled,
            "enabled": hf_enabled,
            "note": hf_note,
        },
        {
            "id": "cdn_direct",
            "label": "Direct CDN",
            "description": "Single-stream download from the primary CDN.",
            "recommended": not hf_enabled,
            "enabled": True,
        },
    ]
    return methods


def build_download_options(app_id: str, install_root: Optional[str] = None) -> Optional[dict]:
    detail = get_steam_detail(app_id)
    if not detail:
        detail = get_steam_summary(app_id) or {}
    if not detail:
        return None
    name = detail.get("name") or str(app_id)
    root = _normalize_install_root(install_root)
    chunk_versions = get_chunk_versions_for_game(app_id, name)
    chunk_size = get_chunk_manifest_size(app_id, name)
    size_bytes = chunk_size or _manifest_size_bytes(app_id) or _estimate_storage_bytes(detail)
    free_bytes, total_bytes = _disk_usage(root)
    return {
        "app_id": str(app_id),
        "name": name,
        "size_bytes": size_bytes,
        "size_label": _format_bytes(size_bytes),
        "methods": list_download_methods(bool(chunk_versions)),
        "versions": chunk_versions or list_download_versions(app_id, detail.get("release_date")),
        "online_fix": get_online_fix_options(app_id),
        "bypass": get_bypass_option(app_id),
        "install_root": root,
        "install_path": root,
        "free_bytes": free_bytes,
        "total_bytes": total_bytes,
    }


def ensure_install_directory(install_root: str, game_name: str, create_subfolder: bool) -> str:
    root = Path(_normalize_install_root(install_root))
    if create_subfolder:
        safe_name = re.sub(r"[^A-Za-z0-9 _-]+", "", game_name).strip() or "Game"
        root = root / safe_name
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


def method_available(method_id: str, methods: Optional[list[dict]] = None) -> bool:
    check = methods if methods is not None else list_download_methods(True)
    for method in check:
        if method.get("id") == method_id:
            return bool(method.get("enabled"))
    return False
