from __future__ import annotations

import ctypes
import csv
import io
import os
import platform
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .steam_catalog import get_catalog_page, get_lua_appids


DEFAULT_REQUIREMENTS: Dict[str, Any] = {
    "min_cpu_cores": 4,
    "min_ram_gb": 8,
    "min_disk_free_gb": 30,
    "min_dx_major": 11,
}


ANTI_CHEAT_FAIL_PROCESSES: Dict[str, str] = {
    "cheatengine-x86_64.exe": "Cheat Engine is running.",
    "cheatengine.exe": "Cheat Engine is running.",
    "x64dbg.exe": "x64dbg debugger is running.",
    "x32dbg.exe": "x32dbg debugger is running.",
    "processhacker.exe": "Process Hacker is running.",
    "procmon.exe": "Process Monitor is running.",
    "procmon64.exe": "Process Monitor is running.",
    "ida64.exe": "IDA debugger is running.",
}


ANTI_CHEAT_WARN_PROCESSES: Dict[str, str] = {
    "rtss.exe": "RivaTuner can conflict with anti-cheat overlays.",
    "msiafterburner.exe": "MSI Afterburner overlay can conflict with anti-cheat hooks.",
    "obs64.exe": "OBS Game Capture can trigger anti-cheat overlays in some games.",
    "weMod.exe": "WeMod may trigger anti-cheat checks.",
    "rewasd.exe": "reWASD can trigger anti-cheat controller checks.",
}


ANTI_CHEAT_WARN_SERVICES: Dict[str, str] = {
    "vgc": "Riot Vanguard service is running.",
    "faceit": "FACEIT anti-cheat service is running.",
    "easyanticheat": "Easy Anti-Cheat service detected.",
    "beservice": "BattlEye service detected.",
}


def _status_rank(value: str) -> int:
    order = {"pass": 0, "warn": 1, "fail": 2}
    return order.get(value, 2)


def _merge_status(values: List[str]) -> str:
    if not values:
        return "pass"
    return max(values, key=_status_rank)


def _build_summary(checks: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {"pass": 0, "warn": 0, "fail": 0}
    for check in checks:
        status = str(check.get("status") or "fail")
        counts[status] = counts.get(status, 0) + 1
    return {
        "status": _merge_status([str(item.get("status") or "fail") for item in checks]),
        "counts": counts,
    }


def _run_text(cmd: List[str], timeout: int = 8) -> str:
    try:
        completed = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        return (completed.stdout or "").strip()
    except Exception:
        return ""


def _powershell(command: str, timeout: int = 8) -> str:
    return _run_text(["powershell", "-NoProfile", "-Command", command], timeout=timeout)


def _detect_ram_gb() -> float:
    if platform.system().lower() == "windows":
        try:
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
                return round(stat.ullTotalPhys / (1024**3), 2)
        except Exception:
            pass
    return 0.0


def _detect_cpu_name() -> str:
    if platform.system().lower() == "windows":
        value = _powershell("(Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)")
        if value:
            return value.strip()
    return platform.processor() or "Unknown CPU"


def _detect_gpu() -> Dict[str, Any]:
    if platform.system().lower() != "windows":
        return {"name": "Unknown GPU", "vram_gb": None}

    output = _powershell(
        "Get-CimInstance Win32_VideoController | "
        "Select-Object Name,AdapterRAM | ConvertTo-Csv -NoTypeInformation"
    )
    if not output:
        return {"name": "Unknown GPU", "vram_gb": None}

    try:
        reader = csv.DictReader(io.StringIO(output))
        rows = [row for row in reader if row.get("Name")]
        if not rows:
            return {"name": "Unknown GPU", "vram_gb": None}
        names = [row.get("Name", "").strip() for row in rows if row.get("Name")]
        vram_values: List[float] = []
        for row in rows:
            raw = (row.get("AdapterRAM") or "").strip()
            if raw.isdigit():
                vram_values.append(round(int(raw) / (1024**3), 2))
        return {
            "name": " | ".join(names[:2]),
            "vram_gb": max(vram_values) if vram_values else None,
        }
    except Exception:
        return {"name": "Unknown GPU", "vram_gb": None}


def _detect_directx_major() -> Optional[int]:
    if platform.system().lower() != "windows":
        return None
    version_output = _run_text(
        ["reg", "query", r"HKLM\SOFTWARE\Microsoft\DirectX", "/v", "Version"],
        timeout=8,
    )
    if not version_output:
        return None

    match = re.search(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", version_output)
    if not match:
        return None
    major = int(match.group(1))
    # Microsoft still stores legacy "4.09..." style values; treat those as DX12-era Windows.
    if major <= 4:
        return 12
    return major


def _disk_free_gb(target_path: str) -> float:
    try:
        usage = shutil.disk_usage(target_path)
        return round(usage.free / (1024**3), 2)
    except Exception:
        return 0.0


def _check_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="otoshi_write_test_", dir=path, delete=False) as handle:
            temp_name = handle.name
            handle.write(b"ok")
        os.remove(temp_name)
        return True
    except Exception:
        return False


def _running_processes() -> set[str]:
    output = _run_text(["tasklist", "/fo", "csv", "/nh"], timeout=8)
    if not output:
        return set()
    names = set()
    try:
        reader = csv.reader(io.StringIO(output))
        for row in reader:
            if not row:
                continue
            names.add(str(row[0]).strip().lower())
    except Exception:
        pass
    return names


def _running_services() -> set[str]:
    output = _powershell(
        "Get-Service | Where-Object {$_.Status -eq 'Running'} | "
        "Select-Object -ExpandProperty Name",
        timeout=10,
    )
    if not output:
        return set()
    return {line.strip().lower() for line in output.splitlines() if line.strip()}


def run_system_check(requirements: Optional[Dict[str, Any]] = None, install_path: Optional[str] = None) -> Dict[str, Any]:
    req = dict(DEFAULT_REQUIREMENTS)
    if requirements:
        req.update(requirements)

    cpu_name = _detect_cpu_name()
    cpu_cores = os.cpu_count() or 0
    ram_gb = _detect_ram_gb()
    gpu_info = _detect_gpu()
    dx_major = _detect_directx_major()
    disk_target = install_path or str(Path.cwd())
    disk_free_gb = _disk_free_gb(disk_target)
    os_version = platform.platform()

    checks: List[Dict[str, Any]] = []
    checks.append(
        {
            "id": "cpu_cores",
            "status": "pass" if cpu_cores >= int(req["min_cpu_cores"]) else "fail",
            "message": f"{cpu_cores} logical cores detected",
            "expected": f">= {int(req['min_cpu_cores'])}",
            "actual": cpu_cores,
        }
    )
    checks.append(
        {
            "id": "ram",
            "status": "pass" if ram_gb >= float(req["min_ram_gb"]) else "fail",
            "message": f"{ram_gb} GB RAM detected",
            "expected": f">= {float(req['min_ram_gb'])} GB",
            "actual": ram_gb,
        }
    )
    checks.append(
        {
            "id": "disk_free",
            "status": "pass" if disk_free_gb >= float(req["min_disk_free_gb"]) else "warn",
            "message": f"{disk_free_gb} GB free disk at {disk_target}",
            "expected": f">= {float(req['min_disk_free_gb'])} GB",
            "actual": disk_free_gb,
        }
    )
    checks.append(
        {
            "id": "directx",
            "status": "pass" if (dx_major or 0) >= int(req["min_dx_major"]) else "warn",
            "message": f"DirectX major: {dx_major if dx_major is not None else 'unknown'}",
            "expected": f">= {int(req['min_dx_major'])}",
            "actual": dx_major,
        }
    )
    os_ok = "windows" in os_version.lower()
    checks.append(
        {
            "id": "os",
            "status": "pass" if os_ok else "warn",
            "message": os_version,
            "expected": "Windows 10/11 recommended",
            "actual": os_version,
        }
    )

    info = {
        "cpu_name": cpu_name,
        "cpu_cores": cpu_cores,
        "ram_gb": ram_gb,
        "gpu_name": gpu_info.get("name"),
        "gpu_vram_gb": gpu_info.get("vram_gb"),
        "directx_major": dx_major,
        "os_version": os_version,
        "disk_free_gb": disk_free_gb,
        "install_path": disk_target,
    }
    return {"summary": _build_summary(checks), "checks": checks, "info": info}


def run_launcher_health_check() -> Dict[str, Any]:
    cwd = Path.cwd()
    backend_data = cwd / "backend" / "app" / "data"
    chunk_dir = cwd / "backend" / "auto_chunk_check_update"
    cache_root = Path(os.getenv("APPDATA") or str(cwd)) / "otoshi_launcher"
    overlay_candidates = [
        cwd / "win64" / "GameOverlayRenderer64.dll",
        cwd / "dist" / "OtoshiLauncher-Portable-v9" / "win64" / "GameOverlayRenderer64.dll",
        cwd / "dist" / "OtoshiLauncher-Portable-v9-no-lua" / "win64" / "GameOverlayRenderer64.dll",
    ]

    checks: List[Dict[str, Any]] = []
    checks.append(
        {
            "id": "backend_data_dir",
            "status": "pass" if backend_data.exists() else "warn",
            "message": f"{backend_data} {'exists' if backend_data.exists() else 'missing'}",
        }
    )
    checks.append(
        {
            "id": "chunk_manifest_dir",
            "status": "pass" if chunk_dir.exists() else "warn",
            "message": f"{chunk_dir} {'exists' if chunk_dir.exists() else 'missing'}",
        }
    )
    checks.append(
        {
            "id": "cache_write_access",
            "status": "pass" if _check_writable(cache_root) else "fail",
            "message": f"Cache dir write test at {cache_root}",
        }
    )
    overlay_exists = any(path.exists() for path in overlay_candidates)
    checks.append(
        {
            "id": "overlay_binary",
            "status": "pass" if overlay_exists else "warn",
            "message": "Overlay runtime binaries detected" if overlay_exists else "Overlay binary not found",
        }
    )

    try:
        lua_count = len(get_lua_appids())
        lua_status = "pass" if lua_count > 0 else "warn"
        checks.append(
            {
                "id": "lua_catalog",
                "status": lua_status,
                "message": f"Lua app index count: {lua_count}",
            }
        )
    except Exception as exc:
        checks.append(
            {
                "id": "lua_catalog",
                "status": "warn",
                "message": f"Lua app index read failed: {exc}",
            }
        )

    return {"summary": _build_summary(checks), "checks": checks}


def run_anticheat_compatibility_check() -> Dict[str, Any]:
    running = _running_processes()
    running_services = _running_services()

    checks: List[Dict[str, Any]] = []
    for process_name, description in ANTI_CHEAT_FAIL_PROCESSES.items():
        if process_name in running:
            checks.append(
                {
                    "id": f"proc_{process_name}",
                    "status": "fail",
                    "message": description,
                }
            )
    for process_name, description in ANTI_CHEAT_WARN_PROCESSES.items():
        if process_name in running:
            checks.append(
                {
                    "id": f"proc_{process_name}",
                    "status": "warn",
                    "message": description,
                }
            )
    for service_name, description in ANTI_CHEAT_WARN_SERVICES.items():
        if service_name in running_services:
            checks.append(
                {
                    "id": f"svc_{service_name}",
                    "status": "warn",
                    "message": description,
                }
            )

    if not checks:
        checks.append(
            {
                "id": "anti_cheat_conflicts",
                "status": "pass",
                "message": "No common anti-cheat conflict process/service detected.",
            }
        )

    return {
        "summary": _build_summary(checks),
        "checks": checks,
        "running_process_count": len(running),
        "running_service_count": len(running_services),
    }


def preload_catalog_cache(limit: int = 48) -> Dict[str, Any]:
    started = time.time()
    checks: List[Dict[str, Any]] = []
    warmed_items = 0
    appids_total = 0

    try:
        appids = get_lua_appids()
        appids_total = len(appids)
        sample = appids[: max(1, int(limit))]
        if sample:
            summaries = get_catalog_page(sample)
            warmed_items = len(summaries)
        checks.append(
            {
                "id": "catalog_preload",
                "status": "pass" if warmed_items > 0 else "warn",
                "message": f"Preloaded {warmed_items} catalog entries (from {appids_total} appids)",
            }
        )
    except Exception as exc:
        checks.append(
            {
                "id": "catalog_preload",
                "status": "warn",
                "message": f"Catalog preload failed: {exc}",
            }
        )

    elapsed_ms = int((time.time() - started) * 1000)
    return {
        "summary": _build_summary(checks),
        "checks": checks,
        "appids_total": appids_total,
        "warmed_items": warmed_items,
        "duration_ms": elapsed_ms,
    }


def run_first_run_diagnostics(
    requirements: Optional[Dict[str, Any]] = None,
    install_path: Optional[str] = None,
    preload_limit: int = 48,
) -> Dict[str, Any]:
    system = run_system_check(requirements=requirements, install_path=install_path)
    health = run_launcher_health_check()
    anticheat = run_anticheat_compatibility_check()
    preload = preload_catalog_cache(limit=preload_limit)

    combined_status = _merge_status(
        [
            system["summary"]["status"],
            health["summary"]["status"],
            anticheat["summary"]["status"],
            preload["summary"]["status"],
        ]
    )

    return {
        "status": combined_status,
        "system": system,
        "health": health,
        "anti_cheat": anticheat,
        "preload": preload,
        "ran_at": int(time.time()),
    }

