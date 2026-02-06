"""
Admin API endpoints for lua file distribution
Only accessible by backend servers, not end users
Supports local files, Hugging Face fallback, and client sync
"""
from fastapi import APIRouter, HTTPException, Header, File, UploadFile
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
import os
import zipfile
import logging
import requests
from pathlib import Path
from io import BytesIO
from typing import List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/lua", tags=["lua-admin"])

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "change-me-in-production")
LUA_FILES_DIR = Path(os.getenv("LUA_FILES_DIR", "./lua_files"))
LUA_VERSION = os.getenv("LUA_VERSION", "1.0.0")
HF_REPO = os.getenv("HF_LUA_REPO", "otoshi/lua-files")


def verify_admin_key(x_api_key: str = Header(None)):
    """Verify API key for admin endpoints"""
    if x_api_key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")


def _get_lua_count() -> int:
    """Get count of lua files"""
    if not LUA_FILES_DIR.exists():
        return 0
    return len(list(LUA_FILES_DIR.glob("*.lua")))


def _get_bundled_size() -> int:
    """Get total size of lua bundle in bytes"""
    if not LUA_FILES_DIR.exists():
        return 0
    total = 0
    for lua_file in LUA_FILES_DIR.glob("*.lua"):
        total += lua_file.stat().st_size
    return total


# ============ Public Endpoints ============

@router.get("/version")
def get_lua_version():
    """Get current lua files version (public)"""
    return {
        "version": LUA_VERSION,
        "lua_count": _get_lua_count(),
        "bundle_size": _get_bundled_size(),
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/stats")
def get_lua_stats():
    """Get lua files statistics (public)"""
    lua_count = _get_lua_count()
    bundle_size = _get_bundled_size()

    return {
        "total_files": lua_count,
        "bundle_size_bytes": bundle_size,
        "bundle_size_mb": round(bundle_size / (1024 * 1024), 2),
        "version": LUA_VERSION,
        "available": lua_count > 0,
    }


# ============ Admin Endpoints ============

@router.get("/bundle.zip")
def download_lua_bundle(x_api_key: str = Header(None)):
    """Download complete lua files bundle as zip (admin)"""
    verify_admin_key(x_api_key)
    
    if not LUA_FILES_DIR.exists() or not list(LUA_FILES_DIR.glob("*.lua")):
        # Fallback to Hugging Face
        try:
            return _fallback_to_huggingface_zip()
        except Exception as e:
            logger.error(f"Failed to fallback to HF: {e}")
            raise HTTPException(status_code=404, detail="No lua files available")

    # Create zip in memory
    zip_buffer = BytesIO()
    file_count = 0
    try:
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for lua_file in sorted(LUA_FILES_DIR.glob("*.lua")):
                zf.write(lua_file, f"lua_files/{lua_file.name}")
                file_count += 1

        logger.info(f"Created zip bundle with {file_count} lua files")
    except Exception as e:
        logger.error(f"Error creating zip: {e}")
        raise HTTPException(status_code=500, detail="Failed to create bundle")

    zip_buffer.seek(0)
    return StreamingResponse(
        iter([zip_buffer.getvalue()]),
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename=lua_bundle_{LUA_VERSION}.zip",
            "X-Lua-Files": str(file_count),
        }
    )


@router.get("/appids")
def get_appids(x_api_key: str = Header(None)):
    """Get list of available appids (admin)"""
    verify_admin_key(x_api_key)
    
    if not LUA_FILES_DIR.exists():
        return {"appids": [], "count": 0}

    appids = []
    for lua_file in LUA_FILES_DIR.glob("*.lua"):
        stem = lua_file.stem.strip()
        if stem.isdigit():
            appids.append(stem)
    
    return {
        "appids": sorted(appids),
        "count": len(appids),
    }


@router.post("/upload")
async def upload_lua_file(
    file: UploadFile = File(...),
    x_api_key: str = Header(None)
):
    """Upload a lua file (admin)"""
    verify_admin_key(x_api_key)

    # Validate file
    if not file.filename.endswith(".lua"):
        raise HTTPException(status_code=400, detail="Only .lua files allowed")

    try:
        LUA_FILES_DIR.mkdir(parents=True, exist_ok=True)

        file_path = LUA_FILES_DIR / file.filename
        contents = await file.read()

        with open(file_path, "wb") as f:
            f.write(contents)

        logger.info(f"Uploaded lua file: {file.filename} ({len(contents)} bytes)")

        return {
            "status": "uploaded",
            "filename": file.filename,
            "size": len(contents),
            "path": str(file_path),
        }
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload file")


@router.delete("/file/{filename}")
def delete_lua_file(filename: str, x_api_key: str = Header(None)):
    """Delete a lua file (admin)"""
    verify_admin_key(x_api_key)

    file_path = LUA_FILES_DIR / filename

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    try:
        file_path.unlink()
        logger.info(f"Deleted lua file: {filename}")

        return {"status": "deleted", "filename": filename}
    except Exception as e:
        logger.error(f"Failed to delete file: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete file")


@router.post("/verify")
def verify_lua_bundle(x_api_key: str = Header(None)):
    """Verify lua bundle integrity (admin)"""
    verify_admin_key(x_api_key)

    if not LUA_FILES_DIR.exists():
        return {
            "valid": False,
            "message": "Lua directory not found",
            "file_count": 0,
        }

    try:
        lua_files = list(LUA_FILES_DIR.glob("*.lua"))

        if not lua_files:
            return {
                "valid": False,
                "message": "No lua files found",
                "file_count": 0,
            }

        total_size = sum(f.stat().st_size for f in lua_files)

        return {
            "valid": True,
            "file_count": len(lua_files),
            "total_size": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "files": [f.name for f in sorted(lua_files)],
        }
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return {
            "valid": False,
            "message": str(e),
        }


# ============ Helper Functions ============

def _fallback_to_huggingface_zip() -> StreamingResponse:
    """Fallback to download zip from Hugging Face"""
    logger.info("Falling back to Hugging Face lua files...")

    hf_zip_url = f"https://huggingface.co/datasets/{HF_REPO}/raw/main/lua-files.zip"

    try:
        response = requests.get(hf_zip_url, timeout=120, stream=True)
        response.raise_for_status()

        def generate():
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    yield chunk

        logger.info("Successfully fallback to HF lua files")

        return StreamingResponse(
            generate(),
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename=lua_bundle_hf_{LUA_VERSION}.zip"
            }
        )
    except Exception as e:
        logger.error(f"HF fallback failed: {e}")
        raise HTTPException(status_code=503, detail="No lua files available from any source")


# ============ Internal Sync Endpoints ============

@router.post("/sync/notify")
def notify_sync_complete(x_api_key: str = Header(None)):
    """Notify backend that client has synced lua files"""
    verify_admin_key(x_api_key)

    logger.info("Lua sync notification received from client")

    return {
        "status": "acknowledged",
        "message": "Sync notification recorded",
        "timestamp": datetime.utcnow().isoformat(),
    }
