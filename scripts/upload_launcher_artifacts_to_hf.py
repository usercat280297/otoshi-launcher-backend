from __future__ import annotations

import argparse
import os
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            stripped = raw.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            cleaned = value.strip().strip('"').strip("'")
            os.environ[key] = cleaned
    except OSError:
        return


def _ensure_env_loaded() -> None:
    # Try project-local env files (safe: only fills missing keys).
    cwd = Path.cwd()
    _load_env_file(cwd / ".env")
    _load_env_file(cwd / "backend" / ".env")


def _discover_artifacts(downloads_dir: Path) -> list[Path]:
    patterns = (
        "Otoshi*.exe",
        "Otoshi*.msi",
        "Otoshi*.zip",
        "Otoshi*.dmg",
        "Otoshi*.AppImage",
        "Otoshi*.tar.gz",
    )
    files: list[Path] = []
    for pattern in patterns:
        files.extend(downloads_dir.glob(pattern))
    deduped = sorted({path.resolve() for path in files if path.is_file()}, key=lambda p: p.stat().st_mtime, reverse=True)
    return deduped


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload launcher artifacts to Hugging Face Hub")
    parser.add_argument("--downloads-dir", default="dist", help="Directory containing launcher artifacts")
    parser.add_argument("--max-items", type=int, default=0, help="Upload at most N artifacts (0 = all)")
    parser.add_argument("--repo-id", required=True, help="Hugging Face repo id, e.g. owner/repo")
    parser.add_argument(
        "--repo-type",
        choices=("dataset", "model", "space"),
        default="dataset",
        help="Hugging Face repo type",
    )
    parser.add_argument("--revision", default="main", help="Revision/branch name")
    parser.add_argument("--subdir", default="launcher", help="Subfolder in repo to store artifacts")
    parser.add_argument(
        "--token",
        default=os.getenv("HUGGINGFACE_TOKEN", os.getenv("HF_TOKEN", "")),
        help="HF token (defaults to env HUGGINGFACE_TOKEN/HF_TOKEN)",
    )
    parser.add_argument(
        "--create-repo",
        action="store_true",
        help="Create repo if it does not exist (requires permission)",
    )
    args = parser.parse_args()

    _ensure_env_loaded()
    token = str(args.token or "").strip() or str(os.getenv("HUGGINGFACE_TOKEN", os.getenv("HF_TOKEN", ""))).strip()
    if not token:
        raise SystemExit("missing HF token: set HUGGINGFACE_TOKEN or pass --token")

    downloads_dir = Path(args.downloads_dir).resolve()
    if not downloads_dir.exists():
        raise SystemExit(f"downloads directory not found: {downloads_dir}")

    artifacts = _discover_artifacts(downloads_dir)
    if not artifacts:
        raise SystemExit("no artifacts found to upload")
    if args.max_items and args.max_items > 0:
        artifacts = artifacts[: args.max_items]

    try:
        from huggingface_hub import HfApi
    except Exception as exc:  # pragma: no cover
        raise SystemExit(f"huggingface_hub is required to upload: {exc}") from exc

    api = HfApi(token=token)

    if args.create_repo:
        api.create_repo(
            repo_id=args.repo_id,
            repo_type=args.repo_type,
            exist_ok=True,
        )

    uploaded = 0
    for path in artifacts:
        path_in_repo = f"{str(args.subdir).strip().strip('/')}/{path.name}".strip("/")
        api.upload_file(
            path_or_fileobj=str(path),
            path_in_repo=path_in_repo,
            repo_id=args.repo_id,
            repo_type=args.repo_type,
            revision=args.revision,
            commit_message=f"Upload launcher artifact: {path.name}",
        )
        uploaded += 1

    print(f"Uploaded {uploaded} artifact(s) to {args.repo_type}:{args.repo_id}@{args.revision}/{args.subdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
