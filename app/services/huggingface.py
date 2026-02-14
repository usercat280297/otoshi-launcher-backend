from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import quote, unquote

import requests
import time

from ..core.config import (
    HF_CONNECT_TIMEOUT_SECONDS,
    HF_MAX_RETRIES,
    HF_CHUNK_MODE,
    HF_CHUNK_PATH_TEMPLATE,
    HF_REPO_ID,
    HF_REPO_TYPE,
    HF_RETRY_BACKOFF_SECONDS,
    HF_REVISION,
    HF_STORAGE_BASE_PATH,
    HF_TIMEOUT_SECONDS,
    HUGGINGFACE_TOKEN,
)


class HuggingFaceChunkError(RuntimeError):
    pass


def _normalize_path(value: str) -> str:
    return value.replace("\\", "/").lstrip("/")


def _encode_path(path: str) -> str:
    segments = []
    for segment in path.split("/"):
        decoded = unquote(segment)
        segments.append(quote(decoded, safe="-_.~"))
    return "/".join(segments)


def _apply_template(template: str, mapping: Dict[str, str]) -> str:
    output = template
    for key, value in mapping.items():
        output = output.replace(f"{{{key}}}", value)
    return output


@dataclass(frozen=True)
class HuggingFaceChunkFetcher:
    def enabled(self) -> bool:
        return bool(HF_REPO_ID)

    def _base_url(self) -> str:
        if HF_REPO_TYPE == "space":
            return f"https://huggingface.co/spaces/{HF_REPO_ID}/resolve/{HF_REVISION}"
        if HF_REPO_TYPE == "model":
            return f"https://huggingface.co/{HF_REPO_ID}/resolve/{HF_REVISION}"
        return f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/{HF_REVISION}"

    def _headers(self, range_header: Optional[str] = None, use_auth: bool = True) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        token = (HUGGINGFACE_TOKEN or "").strip()
        if use_auth and token:
            headers["Authorization"] = f"Bearer {token}"
        if range_header:
            headers["Range"] = range_header
        return headers

    def _build_file_path(self, file_path: str, mapping: Dict[str, str]) -> str:
        base = _apply_template(HF_STORAGE_BASE_PATH, mapping).strip("/")
        normalized = _normalize_path(file_path)
        if base:
            return f"{base}/{normalized}"
        return normalized

    def _build_chunk_path(self, mapping: Dict[str, str]) -> str:
        if not HF_CHUNK_PATH_TEMPLATE:
            raise HuggingFaceChunkError("HF_CHUNK_PATH_TEMPLATE not configured")
        return _normalize_path(_apply_template(HF_CHUNK_PATH_TEMPLATE, mapping))

    def _request_once(
        self,
        url: str,
        headers: Dict[str, str],
    ) -> requests.Response:
        max_retries = max(1, HF_MAX_RETRIES)
        connect_timeout = max(1, HF_CONNECT_TIMEOUT_SECONDS)
        read_timeout = max(1, HF_TIMEOUT_SECONDS)
        timeout = (connect_timeout, read_timeout)
        last_exc: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                return requests.get(
                    url,
                    headers=headers,
                    stream=True,
                    timeout=timeout,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < max_retries:
                    backoff = max(0.0, HF_RETRY_BACKOFF_SECONDS) * attempt
                    if backoff > 0:
                        time.sleep(backoff)
                    continue
                break

        raise HuggingFaceChunkError(
            f"Hugging Face request failed after {max_retries} attempts: {last_exc}"
        )

    def _request(self, path: str, range_header: Optional[str] = None) -> Optional[requests.Response]:
        safe_path = _encode_path(path)
        url = f"{self._base_url().rstrip('/')}/{safe_path}"
        headers = self._headers(range_header)
        response = self._request_once(url, headers)
        if response.status_code in (401, 403) and headers.get("Authorization"):
            response.close()
            response = self._request_once(url, self._headers(range_header, use_auth=False))
        if response.status_code in (200, 206):
            return response
        if response.status_code == 404:
            response.close()
            return None
        response.close()
        raise HuggingFaceChunkError(f"Hugging Face returned {response.status_code}")

    def get_chunk_response(
        self,
        game_id: str,
        slug: str,
        file_id: str,
        file_path: str,
        chunk_index: int,
        size: int,
        chunk_size: int,
    ) -> Optional[requests.Response]:
        if not self.enabled():
            return None

        mapping = {
            "game_id": game_id,
            "slug": slug,
            "file_id": file_id,
            "chunk_index": str(chunk_index),
            "file_path": _normalize_path(file_path),
        }

        mode = HF_CHUNK_MODE.lower().strip()
        normalized_path = mapping["file_path"]
        if mode == "file" or (mode == "auto" and (HF_CHUNK_PATH_TEMPLATE or normalized_path.endswith(".zip"))):
            if HF_CHUNK_PATH_TEMPLATE:
                chunk_path = self._build_chunk_path(mapping)
            else:
                chunk_path = normalized_path
            response = self._request(chunk_path)
            if response is not None or mode == "file":
                return response

        if mode in ("range", "auto"):
            hf_file_path = self._build_file_path(file_path, mapping)
            offset = chunk_index * chunk_size
            end = offset + size - 1
            return self._request(hf_file_path, range_header=f"bytes={offset}-{end}")

        raise HuggingFaceChunkError(f"Unsupported HF_CHUNK_MODE: {HF_CHUNK_MODE}")


huggingface_fetcher = HuggingFaceChunkFetcher()
