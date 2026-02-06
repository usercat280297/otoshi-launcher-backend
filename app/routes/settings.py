from fastapi import APIRouter
from ..schemas import LocaleSettingIn, LocaleSettingOut
from ..services.settings import detect_system_locale, get_user_locale, normalize_locale, set_user_locale

router = APIRouter()


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
