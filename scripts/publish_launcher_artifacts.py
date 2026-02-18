from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

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


def discover_artifacts(downloads_dir: Path) -> list[dict[str, Any]]:
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
                "download_url": f"/launcher-download/file/{path.name}",
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
    args = parser.parse_args()

    downloads_dir = Path(args.downloads_dir).resolve()
    if not downloads_dir.exists():
        raise SystemExit(f"downloads directory not found: {downloads_dir}")

    artifacts = discover_artifacts(downloads_dir)
    if not artifacts:
        raise SystemExit("no artifacts discovered to publish")

    published = publish(args.api_base, args.admin_key, artifacts, args.channel)
    print(json.dumps(published, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
