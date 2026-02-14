from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from ..core.cache import cache_client
from ..core.config import MANIFEST_SOURCE_DIR
from ..db import get_db
from ..models import Game
from ..services.manifest import build_manifest
from ..services.huggingface import HuggingFaceChunkError, huggingface_fetcher

router = APIRouter()
_MANIFEST_CACHE_TTL_SECONDS = 24 * 60 * 60


def _find_file_entry(manifest: dict, file_id: str) -> Optional[dict]:
    for file in manifest.get("files", []):
        if file.get("file_id") == file_id:
            return file
    return None


def _hydrate_manifest_cache(game_id: str, db: Session) -> Optional[dict]:
    cached = cache_client.get_json(f"manifest:{game_id}")
    if isinstance(cached, dict):
        return cached

    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        return None

    manifest = build_manifest(game)
    if not isinstance(manifest, dict):
        return None

    cache_client.set_json(
        f"manifest:{game_id}",
        manifest,
        ttl=_MANIFEST_CACHE_TTL_SECONDS,
    )
    slug = str(manifest.get("slug") or game.slug or "").strip()
    if slug:
        cache_client.set_json(
            f"manifest:{slug}",
            manifest,
            ttl=_MANIFEST_CACHE_TTL_SECONDS,
        )

    return manifest


@router.get("/chunks/{game_id}/{file_id}/{chunk_index}")
def get_chunk(
    game_id: str,
    file_id: str,
    chunk_index: int,
    size: int = Query(..., gt=0, le=2 * 1024 * 1024 * 1024),
    db: Session = Depends(get_db),
):
    manifest = _hydrate_manifest_cache(game_id, db)
    if not manifest:
        raise HTTPException(status_code=404, detail=f"Manifest not found for game_id={game_id}")

    file_entry = None
    if manifest:
        file_entry = _find_file_entry(manifest, file_id)
    if not file_entry:
        cache_client.delete(f"manifest:{game_id}")
        manifest = _hydrate_manifest_cache(game_id, db)
        if manifest:
            file_entry = _find_file_entry(manifest, file_id)
    if not file_entry:
        raise HTTPException(
            status_code=404,
            detail=f"Chunk file not found in manifest (game_id={game_id}, file_id={file_id})",
        )

    file_path = file_entry.get("path") if file_entry else None
    source_path = None
    if file_entry:
        source_path = file_entry.get("source_path") or file_path

    if file_path and MANIFEST_SOURCE_DIR and manifest:
        slug = manifest.get("slug") if manifest else None
        if slug:
            local_source_path = Path(MANIFEST_SOURCE_DIR) / slug / file_path
            if local_source_path.exists():
                offset = chunk_index * manifest.get("chunk_size", 1024 * 1024)

                def file_stream():
                    with local_source_path.open("rb") as handle:
                        handle.seek(offset)
                        remaining = size
                        while remaining > 0:
                            data = handle.read(min(65536, remaining))
                            if not data:
                                break
                            remaining -= len(data)
                            yield data

                return StreamingResponse(file_stream(), media_type="application/octet-stream")

    if source_path and manifest:
        slug = manifest.get("slug") or ""
        chunk_size = int(manifest.get("chunk_size") or 1024 * 1024)
        try:
            response = huggingface_fetcher.get_chunk_response(
                game_id=game_id,
                slug=slug,
                file_id=file_id,
                file_path=source_path,
                chunk_index=chunk_index,
                size=size,
                chunk_size=chunk_size,
            )
        except HuggingFaceChunkError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if response is not None:
            def hf_stream():
                try:
                    for block in response.iter_content(chunk_size=65536):
                        if block:
                            yield block
                finally:
                    response.close()

            headers = {}
            if response.headers.get("Content-Length"):
                headers["Content-Length"] = response.headers["Content-Length"]
            return StreamingResponse(hf_stream(), media_type="application/octet-stream", headers=headers)

    raise HTTPException(
        status_code=502,
        detail=(
            "Chunk source unavailable "
            f"(game_id={game_id}, file_id={file_id}, chunk_index={chunk_index})"
        ),
    )
