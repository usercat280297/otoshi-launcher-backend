import json
from pathlib import Path

from fastapi import APIRouter
from ..schemas import LocaleSettingIn, LocaleSettingOut
from ..services.settings import detect_system_locale, get_user_locale, normalize_locale, set_user_locale

router = APIRouter()
_LOCALE_BUNDLE_DIR = Path(__file__).resolve().parents[1] / "data" / "locales"


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
        "supported": ["en", "vi"],
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
        "supported": ["en", "vi"],
    }


@router.get("/locales/{locale_code}")
def get_locale_bundle(locale_code: str):
    resolved = normalize_locale(locale_code)
    bundle_path = _LOCALE_BUNDLE_DIR / f"{resolved}.json"
    if not bundle_path.exists():
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
