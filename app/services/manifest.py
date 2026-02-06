import json
import math
from hashlib import sha1, sha256
from pathlib import Path
from typing import Dict, List, Optional

from ..core.config import CDN_FALLBACK_URLS, CDN_PRIMARY_URLS, MANIFEST_CACHE_DIR, MANIFEST_SOURCE_DIR
from ..models import Game
from ..services.cdn import iter_chunk_bytes
from ..services.manifest_builder import ManifestBuilder
from ..services.chunk_manifests import build_chunk_manifest
from .native_core import get_native_core

CHUNK_SIZE = 1024 * 1024
COMPRESSION_RATIO = 0.72
SOURCE_DIR = MANIFEST_SOURCE_DIR.strip()
CACHE_DIR = MANIFEST_CACHE_DIR.strip()
PRIMARY_URLS = [url.strip() for url in CDN_PRIMARY_URLS if url.strip()]
FALLBACK_URLS = [url.strip() for url in CDN_FALLBACK_URLS if url.strip()]


def _hash_text(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _file_id(path: str) -> str:
    return sha1(path.encode("utf-8")).hexdigest()[:12]


def _chunk_seed(game_id: str, file_id: str, index: int) -> bytes:
    return f"{game_id}:{file_id}:{index}".encode("utf-8")


def _build_chunk_urls(game_id: str, file_id: str, index: int, size: int) -> tuple[str, list[str]]:
    path = f"/cdn/chunks/{game_id}/{file_id}/{index}?size={size}"
    primary = PRIMARY_URLS[0] if PRIMARY_URLS else "http://localhost:8000"
    url = f"{primary.rstrip('/')}{path}"
    fallbacks = [f"{base.rstrip('/')}{path}" for base in FALLBACK_URLS]
    return url, fallbacks


def _build_stub_manifest(game: Game) -> Dict:
    files = [
        {"path": "game.exe", "size": 8_388_608},
        {"path": "data/pak0.bin", "size": 12_582_912},
        {"path": "data/pak1.bin", "size": 6_291_456},
        {"path": "config/defaults.json", "size": 1_048_576},
    ]

    enriched_files = []
    total_size = 0

    for file in files:
        size = int(file["size"])
        file_id = _file_id(file["path"])
        total_size += size

        file_hasher = sha256()
        chunk_count = max(1, math.ceil(size / CHUNK_SIZE))
        chunks = []
        for index in range(chunk_count):
            is_last = index == chunk_count - 1
            chunk_size = size - (CHUNK_SIZE * (chunk_count - 1)) if is_last else CHUNK_SIZE
            chunk_hasher = sha256()
            seed = _chunk_seed(game.id, file_id, index)

            for block in iter_chunk_bytes(seed, chunk_size):
                chunk_hasher.update(block)
                file_hasher.update(block)

            url, fallbacks = _build_chunk_urls(game.id, file_id, index, chunk_size)
            chunks.append(
                {
                    "index": index,
                    "hash": chunk_hasher.hexdigest(),
                    "size": chunk_size,
                    "url": url,
                    "fallback_urls": fallbacks,
                    "compression": "none",
                }
            )

        enriched_files.append(
            {
                "path": file["path"],
                "size": size,
                "hash": file_hasher.hexdigest(),
                "file_id": file_id,
                "chunks": chunks,
            }
        )

    return {
        "game_id": game.id,
        "slug": game.slug,
        "version": "1.0.0",
        "build_id": _hash_text(f"{game.id}:{game.slug}:1.0.0")[:16],
        "total_size": total_size,
        "compressed_size": int(total_size * COMPRESSION_RATIO),
        "chunk_size": CHUNK_SIZE,
        "files": enriched_files,
    }


def _enrich_native_manifest(game: Game, manifest: Dict) -> Dict:
    files = []
    for file in manifest.get("files", []):
        file_id = file.get("file_id") or _file_id(file.get("path", ""))
        chunks = []
        for chunk in file.get("chunks", []):
            index = int(chunk.get("index", 0))
            size = int(chunk.get("size", 0))
            url, fallbacks = _build_chunk_urls(game.id, file_id, index, size)
            chunks.append(
                {
                    "index": index,
                    "hash": chunk.get("hash"),
                    "size": size,
                    "url": url,
                    "fallback_urls": fallbacks,
                    "compression": chunk.get("compression", "none"),
                }
            )

        files.append(
            {
                "path": file.get("path"),
                "size": file.get("size"),
                "hash": file.get("hash"),
                "file_id": file_id,
                "chunks": chunks,
            }
        )

    manifest.update(
        {
            "game_id": game.id,
            "slug": game.slug,
            "compressed_size": manifest.get("compressed_size")
            or int(manifest.get("total_size", 0) * COMPRESSION_RATIO),
            "chunk_size": manifest.get("chunk_size", CHUNK_SIZE),
            "files": files,
        }
    )
    return manifest


def _native_manifest(game: Game) -> Optional[Dict]:
    if not SOURCE_DIR:
        return None

    source_dir = Path(SOURCE_DIR) / game.slug
    if not source_dir.is_dir():
        return None

    core = get_native_core()
    if not core:
        builder = ManifestBuilder(CHUNK_SIZE)
        manifest = builder.build_manifest(game.id, "1.0.0", source_dir)
        return _enrich_native_manifest(game, manifest)

    cache_dir = Path(CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_path = cache_dir / f"{game.slug}.json"
    core.build_manifest(str(source_dir), str(output_path), CHUNK_SIZE)
    with output_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    return _enrich_native_manifest(game, manifest)


def build_manifest(game: Game) -> Dict:
    chunk_manifest = build_chunk_manifest(game)
    if chunk_manifest is not None:
        return chunk_manifest
    native_manifest = _native_manifest(game)
    if native_manifest is not None:
        return native_manifest

    return _build_stub_manifest(game)
