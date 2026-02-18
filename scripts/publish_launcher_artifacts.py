from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def detect_kind(path: Path) -> str:
    name = path.name.lower()
    if "setup" in name or "installer" in name or name.endswith(".msi") or name.endswith(".dmg"):
        return "installer"
    return "portable"


def detect_platform(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".dmg"):
        return "macos"
    if name.endswith(".appimage") or name.endswith(".tar.gz"):
        return "linux"
    return "windows"


def _encode_path(path_value: str) -> str:
    normalized = str(path_value or "").replace("\\", "/").strip("/")
    if not normalized:
        return ""
    return "/".join(quote(segment, safe="") for segment in normalized.split("/") if segment)


def _build_download_url(filename: str, args: argparse.Namespace) -> str:
    safe_filename = Path(filename).name
    encoded_filename = quote(safe_filename, safe="")

    template = str(args.download_url_template or "").strip()
    if template:
        return template.format(filename=safe_filename, filename_url=encoded_filename)

    base = str(args.download_base_url or "").strip()
    if base:
        return f"{base.rstrip('/')}/{encoded_filename}"

    hf_repo_id = str(args.hf_repo_id or "").strip()
    if hf_repo_id:
        hf_repo_type = str(args.hf_repo_type or "dataset").strip().lower() or "dataset"
        hf_revision = str(args.hf_revision or "main").strip() or "main"
        hf_subdir = _encode_path(str(args.hf_subdir or ""))
        object_path = f"{hf_subdir}/{encoded_filename}" if hf_subdir else encoded_filename

        repo_segment = quote(hf_repo_id, safe="/")
        revision_segment = quote(hf_revision, safe="/")
        if hf_repo_type == "space":
            return f"https://huggingface.co/spaces/{repo_segment}/resolve/{revision_segment}/{object_path}?download=1"
        if hf_repo_type == "model":
            return f"https://huggingface.co/{repo_segment}/resolve/{revision_segment}/{object_path}?download=1"
        return f"https://huggingface.co/datasets/{repo_segment}/resolve/{revision_segment}/{object_path}?download=1"

    return f"/launcher-download/file/{safe_filename}"


def discover_artifacts(downloads_dir: Path, args: argparse.Namespace) -> list[dict[str, Any]]:
    candidates: list[Path] = []
    patterns = (
        "Otoshi*.exe",
        "Otoshi*.msi",
        "Otoshi*.zip",
        "Otoshi*.dmg",
        "Otoshi*.AppImage",
        "Otoshi*.tar.gz",
    )
    for pattern in patterns:
        candidates.extend(downloads_dir.glob(pattern))
    deduped = sorted({path.resolve() for path in candidates if path.is_file()}, key=lambda p: p.stat().st_mtime, reverse=True)

    payload: list[dict[str, Any]] = []
    for path in deduped:
        payload.append(
            {
                "kind": detect_kind(path),
                "filename": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "platform": detect_platform(path),
                "download_url": _build_download_url(path.name, args),
            }
        )
    return payload


def publish(api_base: str, admin_key: str, artifacts: list[dict[str, Any]], channel: str) -> dict[str, Any]:
    response = requests.post(
        f"{api_base.rstrip('/')}/launcher-download/artifacts/publish",
        headers={
            "Content-Type": "application/json",
            "X-API-Key": admin_key,
        },
        json={
            "artifacts": [{**item, "channel": channel} for item in artifacts],
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish launcher artifacts registry")
    parser.add_argument("--api-base", required=True, help="Backend API base URL, e.g. http://127.0.0.1:8000")
    parser.add_argument("--admin-key", required=True, help="ADMIN_API_KEY value")
    parser.add_argument("--downloads-dir", default="dist", help="Directory containing launcher artifacts")
    parser.add_argument("--channel", default="stable", help="Release channel label")
    parser.add_argument(
        "--download-url-template",
        default="",
        help="Custom URL template, supports {filename} and {filename_url}",
    )
    parser.add_argument(
        "--download-base-url",
        default="",
        help="Base URL for hosted binaries, example: https://cdn.example.com/launcher",
    )
    parser.add_argument(
        "--hf-repo-id",
        default=os.getenv("LAUNCHER_HF_REPO_ID", os.getenv("HF_REPO_ID", "")),
        help="Hugging Face repo id for artifact links, example: owner/repo",
    )
    parser.add_argument(
        "--hf-repo-type",
        choices=("dataset", "model", "space"),
        default=os.getenv("LAUNCHER_HF_REPO_TYPE", "dataset"),
        help="Hugging Face repo type",
    )
    parser.add_argument(
        "--hf-revision",
        default=os.getenv("LAUNCHER_HF_REVISION", os.getenv("HF_REVISION", "main")),
        help="Hugging Face revision/branch name",
    )
    parser.add_argument(
        "--hf-subdir",
        default=os.getenv("LAUNCHER_HF_ARTIFACT_SUBDIR", ""),
        help="Optional subfolder in HF repo where launcher artifacts live",
    )
    args = parser.parse_args()

    downloads_dir = Path(args.downloads_dir).resolve()
    if not downloads_dir.exists():
        raise SystemExit(f"downloads directory not found: {downloads_dir}")

    artifacts = discover_artifacts(downloads_dir, args)
    if not artifacts:
        raise SystemExit("no artifacts discovered to publish")

    published = publish(args.api_base, args.admin_key, artifacts, args.channel)
    print(json.dumps(published, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
