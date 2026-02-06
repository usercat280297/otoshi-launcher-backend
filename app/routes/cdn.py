from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..core.cache import cache_client
from ..core.config import MANIFEST_SOURCE_DIR
from ..services.cdn import iter_chunk_bytes
from ..services.huggingface import HuggingFaceChunkError, huggingface_fetcher

router = APIRouter()


def _find_file_entry(manifest: dict, file_id: str) -> Optional[dict]:
    for file in manifest.get("files", []):
        if file.get("file_id") == file_id:
            return file
    return None


@router.get("/chunks/{game_id}/{file_id}/{chunk_index}")
def get_chunk(
    game_id: str,
    file_id: str,
    chunk_index: int,
    size: int = Query(..., gt=0, lt=20 * 1024 * 1024),
):
    manifest = cache_client.get_json(f"manifest:{game_id}")
    file_entry = None
    if manifest:
        file_entry = _find_file_entry(manifest, file_id)
    file_path = file_entry.get("path") if file_entry else None
    source_path = None
    if file_entry:
        source_path = file_entry.get("source_path") or file_path

    if file_path and MANIFEST_SOURCE_DIR:
        slug = manifest.get("slug") if manifest else None
        if slug:
            source_path = Path(MANIFEST_SOURCE_DIR) / slug / file_path
            if source_path.exists():
                offset = chunk_index * manifest.get("chunk_size", 1024 * 1024)

                def file_stream():
                    with source_path.open("rb") as handle:
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

    seed = f"{game_id}:{file_id}:{chunk_index}".encode("utf-8")

    def generated_stream():
        yield from iter_chunk_bytes(seed, size)

    return StreamingResponse(generated_stream(), media_type="application/octet-stream")
