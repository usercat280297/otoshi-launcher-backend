from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1, sha256
from pathlib import Path
from typing import Optional

from ..core.config import CDN_FALLBACK_URLS, CDN_PRIMARY_URLS
from ..services.settings import get_download_settings

# Try to import remote manifests module for hybrid local+remote resolution
try:
    from . import remote_manifests as _remote
    _REMOTE_ENABLED = True
except ImportError:
    _remote = None  # type: ignore
    _REMOTE_ENABLED = False

# Support bundled mode: check CHUNK_MANIFEST_DIR env var first
_DEFAULT_ROOT_DIR = Path(__file__).resolve().parents[2] / "auto_chunk_check_update"
_ROOT_DIR = Path(os.getenv("CHUNK_MANIFEST_DIR", str(_DEFAULT_ROOT_DIR)))

# Support bundled mode: check APP_DATA_DIR env var for manifest map
_DEFAULT_MAP_FILE = Path(__file__).resolve().parents[1] / "data" / "chunk_manifest_map.json"
_MAP_FILE = Path(os.getenv("APP_DATA_DIR", "")) / "chunk_manifest_map.json" if os.getenv("APP_DATA_DIR") else _DEFAULT_MAP_FILE

_NAME_CLEAN = re.compile(r"[^a-z0-9]+")

_PRIMARY_URLS = [url.strip() for url in CDN_PRIMARY_URLS if url.strip()]
_FALLBACK_URLS = [url.strip() for url in CDN_FALLBACK_URLS if url.strip()]
_CHUNK_V2_SOURCE_MODE = os.getenv("CHUNK_V2_SOURCE_MODE", "hybrid_hf_cdn").strip().lower()


@dataclass(frozen=True)
class ChunkManifestMeta:
    game_name: str
    version: str
    folder: str
    size_bytes: int
    original_size_bytes: int
    chunk_count: int
    manifest_path: Path
    updated_ts: float


@dataclass(frozen=True)
class ChunkManifestMatch:
    meta: ChunkManifestMeta
    hf_folder: str
    archive_dir: str
    archive_cleanup: bool


def _normalize_name(value: str) -> str:
    cleaned = _NAME_CLEAN.sub("", value.lower())
    return cleaned or value.lower()


def _parse_timestamp(value: Optional[str], fallback: float) -> float:
    if not value:
        return fallback
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value.strip(), fmt).timestamp()
        except ValueError:
            continue
    return fallback


def _hash_text(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _file_id(path: str) -> str:
    return sha1(path.encode("utf-8")).hexdigest()[:12]


def _build_chunk_urls(game_id: str, file_id: str, index: int, size: int) -> tuple[str, list[str]]:
    path = f"/cdn/chunks/{game_id}/{file_id}/{index}?size={size}"
    primary = _PRIMARY_URLS[0] if _PRIMARY_URLS else "http://localhost:8000"
    url = f"{primary.rstrip('/')}{path}"
    fallbacks = [f"{base.rstrip('/')}{path}" for base in _FALLBACK_URLS]
    return url, fallbacks


def _iter_manifest_paths() -> list[Path]:
    if not _ROOT_DIR.is_dir():
        return []
    manifests: list[Path] = []
    for entry in sorted(_ROOT_DIR.iterdir()):
        if not entry.is_dir():
            continue
        manifests.extend(sorted(entry.glob("manifest*.json")))
    return manifests


def _load_manifest_meta(path: Path) -> Optional[ChunkManifestMeta]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    folder = path.parent.name
    game_name = str(payload.get("game_name") or folder or "").strip()
    version = str(payload.get("version") or path.stem.replace("manifest_", "") or "").strip()
    if not game_name or not version:
        return None

    total_size = int(payload.get("total_size") or 0)
    total_original = int(payload.get("total_original_size") or total_size or 0)
    chunk_count = int(payload.get("total_chunks") or len(payload.get("chunks") or []))

    fallback_ts = path.stat().st_mtime if path.exists() else 0.0
    updated_ts = _parse_timestamp(payload.get("updated_at"), fallback_ts)
    if updated_ts <= 0:
        updated_ts = _parse_timestamp(payload.get("created_at"), fallback_ts)
    if updated_ts <= 0:
        updated_ts = fallback_ts

    return ChunkManifestMeta(
        game_name=game_name,
        version=version,
        folder=folder,
        size_bytes=total_size,
        original_size_bytes=total_original,
        chunk_count=chunk_count,
        manifest_path=path,
        updated_ts=updated_ts,
    )


def _load_manifest_map() -> dict:
    if not _MAP_FILE.exists():
        return {}
    try:
        payload = json.loads(_MAP_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def list_chunk_manifests() -> list[ChunkManifestMeta]:
    items: list[ChunkManifestMeta] = []
    for path in _iter_manifest_paths():
        meta = _load_manifest_meta(path)
        if meta:
            items.append(meta)
    return items


def _select_matching_manifests(
    manifests: list[ChunkManifestMeta],
    game_name: str,
    folder: Optional[str] = None,
) -> list[ChunkManifestMeta]:
    if not manifests:
        return []
    normalized = _normalize_name(game_name)
    matches = []
    for meta in manifests:
        if folder and meta.folder.lower() != folder.lower():
            continue
        if _normalize_name(meta.game_name) == normalized or _normalize_name(meta.folder) == normalized:
            matches.append(meta)
    return matches


def _pick_latest(manifests: list[ChunkManifestMeta]) -> Optional[ChunkManifestMeta]:
    if not manifests:
        return None
    return sorted(manifests, key=lambda item: item.updated_ts, reverse=True)[0]


def _resolve_override(app_id: str, game_name: str) -> Optional[dict]:
    mapping = _load_manifest_map()
    steam_map = mapping.get("steam_app_id") if isinstance(mapping, dict) else None
    if isinstance(steam_map, dict) and app_id in steam_map:
        override = steam_map.get(app_id)
        return override if isinstance(override, dict) else None
    name_map = mapping.get("name") if isinstance(mapping, dict) else None
    if isinstance(name_map, dict):
        key = _normalize_name(game_name)
        override = name_map.get(key)
        return override if isinstance(override, dict) else None
    return None


def _resolve_manifest_from_override(
    manifests: list[ChunkManifestMeta],
    override: dict,
) -> Optional[ChunkManifestMeta]:
    manifest_path = override.get("manifest")
    if isinstance(manifest_path, str) and manifest_path:
        path = Path(manifest_path)
        if not path.is_absolute():
            path = _ROOT_DIR / manifest_path
        return _load_manifest_meta(path) if path.exists() else None

    folder = override.get("folder")
    game_name = override.get("game_name")
    version = override.get("version")
    base_name = str(game_name or folder or "").strip()
    if not base_name:
        return None
    filtered = _select_matching_manifests(manifests, base_name, folder=str(folder) if folder else None)
    if not filtered:
        return None
    if version:
        for meta in filtered:
            if meta.version == version:
                return meta
    return _pick_latest(filtered)


def resolve_chunk_manifest(
    app_id: str,
    game_name: str,
    version_override: Optional[str] = None,
) -> Optional[ChunkManifestMatch]:
    manifests = list_chunk_manifests()
    override = _resolve_override(app_id, game_name)
    if override:
        meta = _resolve_manifest_from_override(manifests, override)
        if not meta:
            return None
        hf_folder = override.get("hf_folder") or f"{meta.folder}/{meta.version}"
        archive_dir = override.get("archive_dir") or ".chunks"
        archive_cleanup = bool(override.get("archive_cleanup", False))
        return ChunkManifestMatch(meta=meta, hf_folder=str(hf_folder), archive_dir=str(archive_dir), archive_cleanup=archive_cleanup)

    filtered = _select_matching_manifests(manifests, game_name)
    if not filtered:
        return None
    if version_override:
        for meta in filtered:
            if meta.version == version_override:
                hf_folder = f"{meta.folder}/{meta.version}"
                return ChunkManifestMatch(meta=meta, hf_folder=hf_folder, archive_dir=".chunks", archive_cleanup=False)
    meta = _pick_latest(filtered)
    if not meta:
        return None
    hf_folder = f"{meta.folder}/{meta.version}"
    return ChunkManifestMatch(meta=meta, hf_folder=hf_folder, archive_dir=".chunks", archive_cleanup=False)


def _slug_to_app_id(slug: str) -> str:
    if slug.startswith("steam-"):
        return slug.split("-", 1)[-1]
    return ""


def get_version_override_for_slug(slug: str) -> Optional[str]:
    settings = get_download_settings()
    overrides = settings.get("version_overrides")
    if isinstance(overrides, dict):
        version = overrides.get(slug)
        if isinstance(version, str) and version.strip():
            return version.strip()
    return None


def get_chunk_versions_for_game(app_id: str, game_name: str) -> list[dict]:
    """Get available versions for a game from local manifests, with remote fallback."""
    manifests = list_chunk_manifests()
    override = _resolve_override(app_id, game_name)
    if override:
        base_name = str(override.get("game_name") or override.get("folder") or "").strip()
        folder = override.get("folder")
        if base_name:
            filtered = _select_matching_manifests(manifests, base_name, folder=str(folder) if folder else None)
        else:
            filtered = manifests
    else:
        filtered = _select_matching_manifests(manifests, game_name)
    
    if not filtered:
        # Try remote manifests if local not found
        if _REMOTE_ENABLED and _remote and _remote.is_remote_enabled():
            print(f"[ChunkManifests] No local manifests for {game_name}, trying remote...")
            return _remote.get_remote_game_versions(game_name)
        return []
    
    latest = _pick_latest(filtered)
    versions = []
    for meta in sorted(filtered, key=lambda item: item.updated_ts, reverse=True):
        versions.append(
            {
                "id": meta.version,
                "label": meta.version,
                "is_latest": meta.version == latest.version if latest else False,
                "size_bytes": meta.original_size_bytes or meta.size_bytes or None,
            }
        )
    return versions


def get_chunk_manifest_size(app_id: str, game_name: str, version_override: Optional[str] = None) -> Optional[int]:
    """Get the size of a manifest, with remote fallback."""
    match = resolve_chunk_manifest(app_id, game_name, version_override)
    if not match:
        # Try remote manifests
        if _REMOTE_ENABLED and _remote and _remote.is_remote_enabled():
            print(f"[ChunkManifests] No local manifest for {game_name}, trying remote size...")
            remote_meta = _remote.resolve_remote_manifest(game_name, version_override)
            if remote_meta:
                size = remote_meta.original_size_bytes or remote_meta.size_bytes
                return size if size > 0 else None
        return None
    size = match.meta.original_size_bytes or match.meta.size_bytes
    return size if size > 0 else None


def build_chunk_manifest(game) -> Optional[dict]:
    """Build chunk manifest for a game from local files, with remote fallback."""
    app_id = _slug_to_app_id(game.slug)
    version_override = get_version_override_for_slug(game.slug)
    match = resolve_chunk_manifest(app_id, game.title, version_override)
    
    if not match:
        # Try remote manifests if local not found
        if _REMOTE_ENABLED and _remote and _remote.is_remote_enabled():
            print(f"[ChunkManifests] No local manifest for {game.title}, building from remote...")
            return _remote.build_remote_chunk_manifest(game, version_override)
        return None

    meta = match.meta
    try:
        payload = json.loads(meta.manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    chunks = payload.get("chunks") or []
    archive_files = set()
    files = []
    archive_dir = match.archive_dir.replace("\\", "/").strip("/")
    if not archive_dir:
        archive_dir = ".chunks"
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        filename = chunk.get("path") or chunk.get("filename")
        if not isinstance(filename, str) or not filename.strip():
            continue
        filename = filename.strip().lstrip("/").replace("\\", "/")
        size = int(chunk.get("size") or chunk.get("compressed_size") or 0)
        chunk_hash = str(chunk.get("hash") or "")
        if size <= 0 or not chunk_hash:
            continue
        file_path = f"{archive_dir}/{filename}"
        file_id = _file_id(file_path)
        url, fallbacks = _build_chunk_urls(game.id, file_id, 0, size)
        source_root = match.hf_folder.strip().rstrip("/").replace("\\", "/")
        files.append(
            {
                "path": file_path,
                "size": size,
                "hash": chunk_hash,
                "file_id": file_id,
                "source_path": f"{source_root}/{filename}",
                "chunks": [
                    {
                        "index": 0,
                        "hash": chunk_hash,
                        "size": size,
                        "url": url,
                        "fallback_urls": fallbacks,
                        "compression": "none",
                    }
                ],
            }
        )
        for entry in chunk.get("files") or []:
            if isinstance(entry, str):
                archive_files.add(entry)
            elif isinstance(entry, dict):
                path_value = entry.get("path")
                if isinstance(path_value, str):
                    archive_files.add(path_value)

    total_size = int(payload.get("total_size") or sum(item.get("size", 0) for item in files))
    total_original = int(payload.get("total_original_size") or total_size)
    build_id = _hash_text(f"{game.id}:{meta.version}")[:16]
    chunk_size = int(float(payload.get("chunk_size_mb") or 0) * 1024 * 1024)

    cleaned_files = []
    for path in archive_files:
        cleaned = path.replace("\\", "/").lstrip("/")
        if cleaned:
            cleaned_files.append(cleaned)

    return {
        "game_id": game.id,
        "slug": game.slug,
        "version": meta.version,
        "build_id": build_id,
        "chunk_size": chunk_size,
        "total_size": total_size,
        "compressed_size": total_size,
        "files": files,
        "install_mode": "archive_chunks",
        "archive_dir": archive_dir,
        "archive_cleanup": match.archive_cleanup,
        "archive_files": sorted(cleaned_files),
        "origin_mode": _CHUNK_V2_SOURCE_MODE,
    }


def get_chunk_v2_source_mode() -> str:
    return _CHUNK_V2_SOURCE_MODE
