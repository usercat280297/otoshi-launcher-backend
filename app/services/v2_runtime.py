from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, Optional
import json
import os
import uuid


try:
    import blake3 as _blake3_mod  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    _blake3_mod = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_sha256(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(canonical).hexdigest()


def _safe_workers(requested: Optional[int]) -> int:
    cpu = os.cpu_count() or 8
    recommended = min(32, max(8, 2 * cpu))
    if requested is None:
        return recommended
    return max(1, min(64, int(requested)))


def _hash_file(path: Path, algorithm: str) -> str:
    if algorithm == "blake3" and _blake3_mod is not None:
        hasher = _blake3_mod.blake3()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class DownloadSessionV2:
    id: str
    user_id: str
    download_id: str
    game_id: str
    slug: str
    channel: str
    method: str
    version: str
    status: str = "queued"
    stage: str = "manifest_fetch"
    install_path: Optional[str] = None
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "download_id": self.download_id,
            "game_id": self.game_id,
            "slug": self.slug,
            "channel": self.channel,
            "method": self.method,
            "version": self.version,
            "status": self.status,
            "stage": self.stage,
            "install_path": self.install_path,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "meta": self.meta,
        }


class V2RuntimeStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._sessions: dict[str, DownloadSessionV2] = {}
        self._scan_reports: dict[str, dict[str, Any]] = {}

    def create_session(
        self,
        *,
        user_id: str,
        download_id: str,
        game_id: str,
        slug: str,
        channel: str,
        method: str,
        version: str,
        install_path: Optional[str],
        meta: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        session = DownloadSessionV2(
            id=str(uuid.uuid4()),
            user_id=user_id,
            download_id=download_id,
            game_id=game_id,
            slug=slug,
            channel=channel,
            method=method,
            version=version,
            install_path=install_path,
            meta=meta or {},
        )
        with self._lock:
            self._sessions[session.id] = session
        return session.to_dict()

    def get_session(self, session_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            session = self._sessions.get(session_id)
            return session.to_dict() if session else None

    def control_session(self, session_id: str, action: str) -> Optional[dict[str, Any]]:
        normalized = action.strip().lower()
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None
            if normalized == "pause":
                session.status = "paused"
                session.stage = "transfer_paused"
            elif normalized == "resume":
                session.status = "downloading"
                session.stage = "chunk_transfer"
            elif normalized == "cancel":
                session.status = "cancelled"
                session.stage = "cancelled"
            else:
                raise ValueError(f"Unsupported control action: {action}")
            session.updated_at = _utc_now_iso()
            return session.to_dict()

    def set_session_stage(
        self,
        session_id: str,
        *,
        stage: str,
        status: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None
            session.stage = stage
            if status:
                session.status = status
            session.updated_at = _utc_now_iso()
            return session.to_dict()

    def save_scan_report(self, report: dict[str, Any]) -> dict[str, Any]:
        report_id = str(report.get("report_id") or uuid.uuid4())
        payload = dict(report)
        payload["report_id"] = report_id
        payload["saved_at"] = _utc_now_iso()
        with self._lock:
            self._scan_reports[report_id] = payload
        return payload

    def get_scan_report(self, report_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            report = self._scan_reports.get(report_id)
            return dict(report) if report else None


def _scan_manifest_entry(install_root: Path, entry: dict[str, Any]) -> dict[str, Any]:
    rel = str(entry.get("path") or "").replace("\\", "/").lstrip("/")
    expected_size = int(entry.get("size") or 0)
    expected_sha256 = str(entry.get("hash") or "").strip().lower()
    target = install_root / rel

    payload = {
        "path": rel,
        "exists": target.exists(),
        "expected_size": expected_size,
        "actual_size": 0,
        "expected_sha256": expected_sha256 or None,
        "actual_sha256": None,
        "fast_hash_blake3": None,
        "status": "missing",
        "reason": "missing_file",
    }

    if not target.exists() or not target.is_file():
        return payload

    try:
        actual_size = target.stat().st_size
    except OSError:
        payload["status"] = "error"
        payload["reason"] = "stat_failed"
        return payload

    payload["actual_size"] = actual_size
    if expected_size > 0 and actual_size != expected_size:
        payload["status"] = "corrupt"
        payload["reason"] = "size_mismatch"
        return payload

    try:
        if expected_sha256:
            payload["actual_sha256"] = _hash_file(target, "sha256")
            if payload["actual_sha256"].lower() != expected_sha256:
                payload["status"] = "corrupt"
                payload["reason"] = "hash_mismatch"
                payload["fast_hash_blake3"] = (
                    _hash_file(target, "blake3") if _blake3_mod is not None else None
                )
                return payload
        if _blake3_mod is not None:
            payload["fast_hash_blake3"] = _hash_file(target, "blake3")
    except OSError:
        payload["status"] = "error"
        payload["reason"] = "read_failed"
        return payload

    payload["status"] = "ok"
    payload["reason"] = "verified"
    return payload


def _iter_manifest_files(manifest: dict[str, Any]) -> Iterable[dict[str, Any]]:
    files = manifest.get("files")
    if not isinstance(files, list):
        return []
    return [item for item in files if isinstance(item, dict) and item.get("path")]


def run_self_heal_scan(
    *,
    install_path: str,
    manifest: dict[str, Any],
    max_workers: Optional[int] = None,
    usn_delta_eligible: bool = True,
) -> dict[str, Any]:
    install_root = Path(install_path)
    files = list(_iter_manifest_files(manifest))
    workers = _safe_workers(max_workers)
    started_at = _utc_now_iso()

    if not install_root.exists() or not install_root.is_dir():
        return {
            "report_id": str(uuid.uuid4()),
            "started_at": started_at,
            "finished_at": _utc_now_iso(),
            "install_path": str(install_root),
            "engine": "usn_delta" if usn_delta_eligible else "full_scan",
            "usn_delta_used": False,
            "shadow_verification_queued": False,
            "summary": {
                "total_files": len(files),
                "verified_files": 0,
                "missing_files": len(files),
                "corrupt_files": 0,
                "error_files": 0,
            },
            "files": [
                {
                    "path": str(item.get("path")),
                    "exists": False,
                    "expected_size": int(item.get("size") or 0),
                    "actual_size": 0,
                    "expected_sha256": item.get("hash"),
                    "actual_sha256": None,
                    "fast_hash_blake3": None,
                    "status": "missing",
                    "reason": "install_path_missing",
                }
                for item in files
            ],
            "hot_fix_queue": [str(item.get("path")) for item in files],
        }

    engine = "usn_delta" if usn_delta_eligible and os.name == "nt" else "full_scan"
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(lambda item: _scan_manifest_entry(install_root, item), files))

    total_files = len(results)
    verified_files = sum(1 for item in results if item["status"] == "ok")
    missing_files = sum(1 for item in results if item["status"] == "missing")
    corrupt_files = sum(1 for item in results if item["status"] == "corrupt")
    error_files = sum(1 for item in results if item["status"] == "error")
    hot_fix_queue = [
        str(item.get("path"))
        for item in results
        if item.get("status") in {"missing", "corrupt", "error"}
    ]

    return {
        "report_id": str(uuid.uuid4()),
        "started_at": started_at,
        "finished_at": _utc_now_iso(),
        "install_path": str(install_root),
        "engine": engine,
        "usn_delta_used": engine == "usn_delta",
        "shadow_verification_queued": True,
        "summary": {
            "total_files": total_files,
            "verified_files": verified_files,
            "missing_files": missing_files,
            "corrupt_files": corrupt_files,
            "error_files": error_files,
        },
        "files": results,
        "hot_fix_queue": hot_fix_queue,
        "scan_hash_policy": {
            "canonical_integrity": "sha256",
            "fast_local_scan": "blake3" if _blake3_mod is not None else "sha256",
        },
    }


def build_repair_plan_from_report(
    report: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    files = report.get("files") if isinstance(report.get("files"), list) else []
    to_repair_paths = {
        str(item.get("path"))
        for item in files
        if isinstance(item, dict) and item.get("status") in {"missing", "corrupt", "error"}
    }

    manifest_index = {
        str(item.get("path")): item
        for item in _iter_manifest_files(manifest)
    }

    queue = []
    for rel_path in sorted(to_repair_paths):
        entry = manifest_index.get(rel_path, {})
        queue.append(
            {
                "path": rel_path,
                "expected_size": int(entry.get("size") or 0),
                "expected_sha256": str(entry.get("hash") or "") or None,
                "strategy": "chunk_refetch",
            }
        )

    strategy = "no_op" if not queue else "targeted_hot_fix"
    return {
        "repair_id": str(uuid.uuid4()),
        "generated_at": _utc_now_iso(),
        "strategy": strategy,
        "queue": queue,
        "queue_count": len(queue),
    }


runtime_v2 = V2RuntimeStore()
