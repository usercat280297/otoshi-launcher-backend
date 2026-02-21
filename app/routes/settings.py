import json
import sys
from pathlib import Path

from fastapi import APIRouter
from ..schemas import LocaleSettingIn, LocaleSettingOut
from ..services.settings import detect_system_locale, get_user_locale, normalize_locale, set_user_locale

router = APIRouter()
_LOCALE_BUNDLE_DIR = Path(__file__).resolve().parents[1] / "data" / "locales"
_SUPPORTED_LOCALES = ("en", "vi")


def _locale_dir_candidates() -> list[Path]:
    candidates: list[Path] = []
    cwd = Path.cwd()
    candidates.extend(
        [
            _LOCALE_BUNDLE_DIR,
            cwd / "config" / "locales",
            cwd / "resources" / "backend" / "config" / "locales",
            cwd / "backend" / "app" / "data" / "locales",
        ]
    )

    exe_path = Path(sys.executable).resolve()
    exe_dir = exe_path.parent
    candidates.extend(
        [
            exe_dir / "config" / "locales",
            exe_dir / ".." / ".." / "config" / "locales",
            exe_dir / ".." / ".." / "backend" / "app" / "data" / "locales",
        ]
    )

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        base = Path(str(meipass))
        candidates.extend(
            [
                base / "app" / "data" / "locales",
                base / "config" / "locales",
            ]
        )

    unique: list[Path] = []
    seen: set[str] = set()
    for raw in candidates:
        try:
            resolved = raw.resolve(strict=False)
        except Exception:
            resolved = raw
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _resolve_locale_bundle_file(locale_code: str) -> Path | None:
    for base_dir in _locale_dir_candidates():
        candidate = base_dir / f"{locale_code}.json"
        if candidate.exists():
            return candidate
    return None


@router.get("/locale", response_model=LocaleSettingOut)
def get_locale_setting():
    user_locale = get_user_locale()
    system_locale_raw = detect_system_locale()
    system_locale = normalize_locale(system_locale_raw)
    if user_locale:
        resolved = user_locale
        source = "user"
    elif system_locale:
        resolved = system_locale
        source = "system"
    else:
        resolved = "en"
        source = "default"
    return {
        "locale": resolved,
        "source": source,
        "system_locale": system_locale,
        "supported": list(_SUPPORTED_LOCALES),
    }


@router.post("/locale", response_model=LocaleSettingOut)
def set_locale_setting(payload: LocaleSettingIn):
    resolved = set_user_locale(payload.locale)
    system_locale_raw = detect_system_locale()
    system_locale = normalize_locale(system_locale_raw)
    return {
        "locale": resolved,
        "source": "user",
        "system_locale": system_locale,
        "supported": list(_SUPPORTED_LOCALES),
    }


@router.get("/locales/{locale_code}")
def get_locale_bundle(locale_code: str):
    resolved = normalize_locale(locale_code)
    bundle_path = _resolve_locale_bundle_file(resolved)
    if bundle_path is None and resolved != "en":
        bundle_path = _resolve_locale_bundle_file("en")
    if bundle_path is None:
        return {
            "locale": resolved,
            "messages": {},
            "updated_at": None,
            "source": "empty",
        }

    try:
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}

    messages = payload.get("messages") if isinstance(payload, dict) else {}
    if not isinstance(messages, dict):
        messages = {}

    return {
        "locale": resolved,
        "messages": messages,
        "updated_at": payload.get("updated_at") if isinstance(payload, dict) else None,
        "source": "file",
    }
