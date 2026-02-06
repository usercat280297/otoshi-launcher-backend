"""
Remote Manifest Service - Fetches game manifests from HuggingFace repository.
This allows the launcher to automatically get new games and versions without rebuilding.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1, sha256
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import quote

import requests

from ..core.config import (
    CDN_FALLBACK_URLS,
    CDN_PRIMARY_URLS,
    HF_REPO_ID,
    HF_REPO_TYPE,
    HF_REVISION,
    HUGGINGFACE_TOKEN,
)

# Cache for remote manifests
_manifest_cache: Dict[str, Any] = {}
_manifest_list_cache: Optional[List['RemoteManifestMeta']] = None
_CACHE_TTL_SECONDS = 300  # 5 minutes
_last_cache_update: float = 0.0

_PRIMARY_URLS = [url.strip() for url in CDN_PRIMARY_URLS if url.strip()]
_FALLBACK_URLS = [url.strip() for url in CDN_FALLBACK_URLS if url.strip()]
_NAME_CLEAN = re.compile(r"[^a-z0-9]+")


def _normalize_name(value: str) -> str:
    cleaned = _NAME_CLEAN.sub("", value.lower())
    return cleaned or value.lower()


def _hash_text(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _file_id(path: str) -> str:
    return sha1(path.encode("utf-8")).hexdigest()[:12]


def _build_chunk_urls(game_id: str, file_id: str, index: int, size: int) -> Tuple[str, List[str]]:
    path = f"/cdn/chunks/{game_id}/{file_id}/{index}?size={size}"
    primary = _PRIMARY_URLS[0] if _PRIMARY_URLS else "http://localhost:8000"
    url = f"{primary.rstrip('/')}{path}"
    fallbacks = [f"{base.rstrip('/')}{path}" for base in _FALLBACK_URLS]
    return url, fallbacks


def _hf_base_url() -> str:
    """Build HuggingFace API base URL."""
    if HF_REPO_TYPE == "space":
        return f"https://huggingface.co/spaces/{HF_REPO_ID}/resolve/{HF_REVISION}"
    if HF_REPO_TYPE == "model":
        return f"https://huggingface.co/{HF_REPO_ID}/resolve/{HF_REVISION}"
    return f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/{HF_REVISION}"


def _hf_api_url() -> str:
    """Build HuggingFace API listing URL."""
    if HF_REPO_TYPE == "space":
        return f"https://huggingface.co/api/spaces/{HF_REPO_ID}/tree/{HF_REVISION}"
    if HF_REPO_TYPE == "model":
        return f"https://huggingface.co/api/models/{HF_REPO_ID}/tree/{HF_REVISION}"
    return f"https://huggingface.co/api/datasets/{HF_REPO_ID}/tree/{HF_REVISION}"


def _hf_headers() -> Dict[str, str]:
    """Build request headers with auth token."""
    headers: Dict[str, str] = {}
    token = HUGGINGFACE_TOKEN.strip() if HUGGINGFACE_TOKEN else ""
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _hf_get(url: str, timeout: int) -> requests.Response:
    """GET with optional auth; retry without auth on 401/403."""
    headers = _hf_headers()
    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code in (401, 403) and headers.get("Authorization"):
        response.close()
        response = requests.get(url, headers={}, timeout=timeout)
    return response


def _encode_path(path: str) -> str:
    """URL-encode path segments."""
    segments: List[str] = []
    for segment in path.split("/"):
        segments.append(quote(segment, safe="-_.~"))
    return "/".join(segments)


def is_remote_enabled() -> bool:
    """Check if remote manifest fetching is enabled."""
    return bool(HF_REPO_ID)


def list_remote_game_folders() -> List[str]:
    """List all game folders in the HuggingFace repository."""
    if not is_remote_enabled():
        return []
    
    try:
        response = _hf_get(_hf_api_url(), timeout=15)
        if response.status_code != 200:
            print(f"[Remote Manifest] Failed to list repo: {response.status_code}")
            return []
        
        items = response.json()
        folders: List[str] = []
        for item in items:
            if item.get("type") == "directory":
                folder_path = item.get("path", "")
                if isinstance(folder_path, str):
                    folders.append(folder_path)
        return [f for f in folders if f]
    except (requests.RequestException, ValueError) as e:
        print(f"[Remote Manifest] Error listing folders: {e}")
        return []


def find_manifest_files(folder: str) -> List[str]:
    """Find manifest JSON files in a game folder."""
    if not is_remote_enabled():
        return []
    
    try:
        url = f"{_hf_api_url()}/{_encode_path(folder)}"
        response = _hf_get(url, timeout=15)
        if response.status_code != 200:
            return []
        
        items = response.json()
        manifests: List[str] = []
        for item in items:
            if item.get("type") == "file":
                path = item.get("path", "")
                if isinstance(path, str) and path.endswith(".json") and "manifest" in path.lower():
                    manifests.append(path)
        return manifests
    except (requests.RequestException, ValueError) as e:
        print(f"[Remote Manifest] Error finding manifests in {folder}: {e}")
        return []


def fetch_remote_manifest(manifest_path: str) -> Optional[Dict[str, Any]]:
    """Fetch a manifest JSON from HuggingFace."""
    
    # Check cache first
    cache_key = manifest_path
    if cache_key in _manifest_cache:
        cached = _manifest_cache[cache_key]
        if time.time() - cached.get("_cached_at", 0) < _CACHE_TTL_SECONDS:
            return cached.get("data")
    
    if not is_remote_enabled():
        return None
    
    try:
        url = f"{_hf_base_url()}/{_encode_path(manifest_path)}"
        response = _hf_get(url, timeout=30)

        if response.status_code in (401, 403):
            print("[Remote Manifest] Hugging Face authentication failed - check token")
            return None

        if response.status_code != 200:
            print(f"[Remote Manifest] Failed to fetch {manifest_path}: {response.status_code}")
            return None
        
        data: Dict[str, Any] = response.json()
        _manifest_cache[cache_key] = {
            "data": data,
            "_cached_at": time.time()
        }
        return data
    except requests.exceptions.Timeout:
        print(f"[Remote Manifest] Request timeout fetching {manifest_path}")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"[Remote Manifest] Connection error: {e}")
        return None
    except (ValueError, KeyError) as e:
        print(f"[Remote Manifest] Error fetching {manifest_path}: {e}")
        return None


@dataclass(frozen=True)
class RemoteManifestMeta:
    game_name: str
    version: str
    folder: str
    hf_path: str
    size_bytes: int
    original_size_bytes: int
    chunk_count: int
    updated_ts: float


def list_remote_manifests(force_refresh: bool = False) -> List[RemoteManifestMeta]:
    """List all available manifests from HuggingFace repository."""
    global _manifest_list_cache, _last_cache_update
    if not force_refresh and _manifest_list_cache is not None:
        if time.time() - _last_cache_update < _CACHE_TTL_SECONDS:
            return list(_manifest_list_cache)
    
    if not is_remote_enabled():
        return []
    
    manifests: List[RemoteManifestMeta] = []
    folders = list_remote_game_folders()
    for folder in folders:
        manifest_files = find_manifest_files(folder)
        for mf_path in manifest_files:
            data = fetch_remote_manifest(mf_path)
            if not data:
                continue
            
            game_name = str(data.get("game_name") or folder).strip()
            version = str(data.get("version") or "").strip()
            if not version:
                # Try to extract from filename
                match = re.search(r"manifest[_\s]*(.+)\.json", mf_path, re.IGNORECASE)
                if match:
                    version = match.group(1).strip()
            
            total_size = int(data.get("total_size") or 0)
            original_size = int(data.get("total_original_size") or total_size)
            chunk_count = int(data.get("total_chunks") or len(data.get("chunks") or []))
            
            updated_str = data.get("updated_at") or data.get("created_at")
            updated_ts: float = 0.0
            if updated_str and isinstance(updated_str, str):
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        updated_ts = datetime.strptime(updated_str.strip(), fmt).timestamp()
                        break
                    except ValueError:
                        continue
            
            manifests.append(RemoteManifestMeta(
                game_name=game_name,
                version=version,
                folder=folder,
                hf_path=mf_path,
                size_bytes=total_size,
                original_size_bytes=original_size,
                chunk_count=chunk_count,
                updated_ts=updated_ts
            ))
    
    _manifest_list_cache = manifests
    _last_cache_update = time.time()
    return manifests


def resolve_remote_manifest(
    game_name: str,
    version_override: Optional[str] = None
) -> Optional[RemoteManifestMeta]:
    """Find matching manifest from remote repository."""
    manifests = list_remote_manifests()
    if not manifests:
        return None
    
    normalized = _normalize_name(game_name)
    matches: List[RemoteManifestMeta] = []
    
    for meta in manifests:
        if _normalize_name(meta.game_name) == normalized or _normalize_name(meta.folder) == normalized:
            matches.append(meta)
    
    if not matches:
        return None
    
    if version_override:
        for meta in matches:
            if meta.version == version_override:
                return meta
    
    # Return latest by updated_ts
    return sorted(matches, key=lambda m: m.updated_ts, reverse=True)[0]


def build_remote_chunk_manifest(game: Any, version_override: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Build manifest from remote HuggingFace data."""
    meta = resolve_remote_manifest(game.title, version_override)
    if not meta:
        return None
    
    # Fetch full manifest data
    payload = fetch_remote_manifest(meta.hf_path)
    if not payload:
        return None
    
    chunks: List[Dict[str, Any]] = payload.get("chunks") or []
    archive_dir = ".chunks"
    archive_files: set[str] = set()
    files: List[Dict[str, Any]] = []
    
    # Determine hf_folder from manifest path
    hf_folder = str(Path(meta.hf_path).parent).replace("\\", "/")
    
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
        source_root = hf_folder.strip().rstrip("/")
        
        files.append({
            "path": file_path,
            "size": size,
            "hash": chunk_hash,
            "file_id": file_id,
            "source_path": f"{source_root}/{filename}",
            "chunks": [{
                "index": 0,
                "hash": chunk_hash,
                "size": size,
                "url": url,
                "fallback_urls": fallbacks,
                "compression": "none",
            }]
        })
        
        chunk_files: List[Any] = chunk.get("files") or []
        for entry in chunk_files:
            if isinstance(entry, str):
                archive_files.add(entry)
            elif isinstance(entry, dict):
                path_value = entry.get("path")
                if isinstance(path_value, str):
                    archive_files.add(path_value)
    
    total_size = int(payload.get("total_size") or sum(f.get("size", 0) for f in files))
    _total_original = int(payload.get("total_original_size") or total_size)
    build_id = _hash_text(f"{game.id}:{meta.version}")[:16]
    chunk_size = int(float(payload.get("chunk_size_mb") or 0) * 1024 * 1024)
    
    cleaned_files: List[str] = []
    for path in archive_files:
        cleaned = path.replace("\\", "/").lstrip("/")
        if cleaned:
            cleaned_files.append(cleaned)
    
    result: Dict[str, Any] = {
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
        "archive_cleanup": False,
        "archive_files": sorted(cleaned_files),
    }
    return result

def get_remote_game_versions(game_name: str) -> List[Dict[str, Any]]:
    """Get available versions for a game from remote repository."""
    manifests = list_remote_manifests()
    normalized = _normalize_name(game_name)
    
    matches: List[RemoteManifestMeta] = []
    for meta in manifests:
        if _normalize_name(meta.game_name) == normalized or _normalize_name(meta.folder) == normalized:
            matches.append(meta)
    
    if not matches:
        return []
    
    latest = sorted(matches, key=lambda m: m.updated_ts, reverse=True)[0]
    
    versions: List[Dict[str, Any]] = []
    for meta in sorted(matches, key=lambda m: m.updated_ts, reverse=True):
        versions.append({
            "id": meta.version,
            "label": meta.version,
            "is_latest": meta.version == latest.version,
            "size_bytes": meta.original_size_bytes or meta.size_bytes or None,
        })
    
    return versions


def clear_cache() -> None:
    """Clear all cached manifests."""
    global _manifest_list_cache, _last_cache_update
    _manifest_cache.clear()
    _manifest_list_cache = None
    _last_cache_update = 0.0
