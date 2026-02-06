import base64
import secrets
import traceback
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import fastapi.responses
import requests
from requests import RequestException
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from jose import JWTError, jwt
from sqlalchemy.orm import Session
from pathlib import Path

from ..core.cache import cache_client
from ..core.config import (
    ALGORITHM,
    CORS_ORIGINS,
    DISCORD_OAUTH_AUTH_URL,
    DISCORD_OAUTH_CLIENT_ID,
    DISCORD_OAUTH_CLIENT_SECRET,
    DISCORD_OAUTH_SCOPES,
    DISCORD_OAUTH_TOKEN_URL,
    DISCORD_OAUTH_USERINFO_URL,
    EPIC_OAUTH_AUTH_URL,
    EPIC_OAUTH_CLIENT_ID,
    EPIC_OAUTH_CLIENT_SECRET,
    EPIC_OAUTH_SCOPES,
    EPIC_OAUTH_TOKEN_URL,
    EPIC_OAUTH_USERINFO_URL,
    FRONTEND_BASE_URL,
    GOOGLE_OAUTH_AUTH_URL,
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_SCOPES,
    GOOGLE_OAUTH_TOKEN_URL,
    GOOGLE_OAUTH_USERINFO_URL,
    OAUTH_CALLBACK_BASE_URL,
    OAUTH_DEBUG_ERRORS,
    OAUTH_STATE_TTL_SECONDS,
    SECRET_KEY,
    SESSION_TTL_SECONDS,
    STEAM_OPENID_URL,
    STEAM_WEB_API_KEY,
    STEAM_WEB_API_URL,
)
from ..core.security import create_access_token, create_refresh_token, get_password_hash, verify_password
from ..db import get_db
from ..models import OAuthIdentity, User
from ..schemas import (
    OAuthExchangeIn,
    OAuthProviderOut,
    Token,
    TokenRefresh,
    UserCreate,
    UserLogin,
    UserOut,
)
from .deps import get_current_user
from ..utils.admin import ensure_admin_role

router = APIRouter()

OAUTH_PROVIDER_LABELS = {
    "steam": "Steam",
    "epic": "Epic Games",
    "google": "Google",
    "discord": "Discord",
}
_OAUTH_LOG_PATH = Path(__file__).resolve().parents[2] / "storage" / "oauth_errors.log"


def _append_query(url: str, params: dict[str, str]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query.update(params)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _safe_redirect(target: str) -> bool:
    # Allow Tauri deep-link protocols
    if target.startswith("tauri://localhost"):
        return True
    if target.startswith("otoshi://"):
        return True
    try:
        target_parsed = urlparse(target)
    except ValueError:
        return False
    if not target_parsed.scheme or not target_parsed.netloc:
        return False
    allowed_origins = {FRONTEND_BASE_URL, *CORS_ORIGINS}
    for origin in allowed_origins:
        if origin.startswith("tauri://localhost") or origin.startswith("otoshi://"):
            continue
        origin_parsed = urlparse(origin)
        if (
            origin_parsed.scheme == target_parsed.scheme
            and origin_parsed.netloc == target_parsed.netloc
        ):
            return True
    return False


def _build_callback_url(request: Request, provider: str) -> str:
    callback_url = str(request.url_for("oauth_callback", provider=provider))
    if not OAUTH_CALLBACK_BASE_URL:
        return callback_url
    try:
        base = urlparse(OAUTH_CALLBACK_BASE_URL)
    except ValueError:
        return callback_url
    if not base.scheme or not base.netloc:
        return callback_url
    parsed = urlparse(callback_url)
    base_path = base.path.rstrip("/") if base.path not in ("", "/") else ""
    new_path = f"{base_path}{parsed.path}" if base_path else parsed.path
    return urlunparse(parsed._replace(scheme=base.scheme, netloc=base.netloc, path=new_path))


def _oauth_state_key(state: str) -> str:
    return f"oauth:state:{state}"


def _oauth_poll_key(request_id: str) -> str:
    return f"oauth:poll:{request_id}"


def _oauth_exchange_key(code: str) -> str:
    return f"oauth:exchange:{code}"



def _provider_enabled(provider: str) -> bool:
    if provider == "google":
        return bool(GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET)
    if provider == "epic":
        return bool(EPIC_OAUTH_CLIENT_ID and EPIC_OAUTH_CLIENT_SECRET)
    if provider == "discord":
        return bool(DISCORD_OAUTH_CLIENT_ID and DISCORD_OAUTH_CLIENT_SECRET)
    if provider == "steam":
        return True
    return False


def _log_oauth_error(provider: str, request: Request, exc: Exception) -> None:
    try:
        _OAUTH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().isoformat()
        line = (
            f"[{timestamp}] provider={provider} path={request.url.path} "
            f"query={request.url.query} error={exc}\n"
        )
        with _OAUTH_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write(traceback.format_exc())
            handle.write("\n")
    except OSError:
        return


def _safe_json_response(response: requests.Response, context: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{context} returned invalid JSON",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail=f"{context} returned invalid payload")
    return payload


def _extract_oauth_error(payload: dict[str, Any], fallback: str) -> str:
    return (
        payload.get("error_description")
        or payload.get("error")
        or payload.get("message")
        or fallback
    )


def _issue_tokens(db: Session, user: User) -> dict[str, Any]:
    access = create_access_token(user.id)
    refresh = create_refresh_token(user.id)
    user.last_login = datetime.utcnow()
    db.commit()
    cache_client.set_session(user.id, access, SESSION_TTL_SECONDS)
    return {"access_token": access, "refresh_token": refresh, "token_type": "bearer", "user": user}


def _generate_placeholder_email(provider: str, provider_user_id: str) -> str:
    return f"{provider}_{provider_user_id}@otoshi.local"


def _normalize_username(value: str) -> str:
    cleaned = "".join(char if char.isalnum() else "_" for char in value.lower())
    cleaned = "_".join(segment for segment in cleaned.split("_") if segment)
    return cleaned[:32]


def _get_or_create_user(db: Session, oauth_user: dict[str, Any]) -> User:
    provider = oauth_user["provider"]
    provider_user_id = oauth_user["provider_user_id"]
    email = oauth_user.get("email")
    display_name = oauth_user.get("display_name")
    avatar_url = oauth_user.get("avatar_url")

    identity = (
        db.query(OAuthIdentity)
        .filter(
            OAuthIdentity.provider == provider,
            OAuthIdentity.provider_user_id == provider_user_id,
        )
        .first()
    )
    if identity:
        user = identity.user
        if display_name and not user.display_name:
            user.display_name = display_name
        if avatar_url and not user.avatar_url:
            user.avatar_url = avatar_url
        identity.email = email or identity.email
        identity.display_name = display_name or identity.display_name
        identity.avatar_url = avatar_url or identity.avatar_url
        db.commit()
        return user

    user = None
    if email:
        user = db.query(User).filter(User.email == email).first()

    if not user:
        base_username = _normalize_username(display_name or provider_user_id or provider)
        if len(base_username) < 3:
            base_username = f"{provider}_{provider_user_id[-6:]}"
        username = base_username
        suffix = 1
        while db.query(User).filter(User.username == username).first():
            suffix += 1
            username = f"{base_username}{suffix}"

        resolved_email = email or _generate_placeholder_email(provider, provider_user_id)
        while db.query(User).filter(User.email == resolved_email).first():
            resolved_email = _generate_placeholder_email(provider, f"{provider_user_id}-{suffix}")
            suffix += 1

        user = User(
            email=resolved_email,
            username=username,
            display_name=display_name or username,
            avatar_url=avatar_url,
            password_hash=get_password_hash(secrets.token_urlsafe(16)),
            is_verified=bool(email),
        )
        db.add(user)
        db.flush()
    else:
        if display_name and not user.display_name:
            user.display_name = display_name
        if avatar_url and not user.avatar_url:
            user.avatar_url = avatar_url

    identity = OAuthIdentity(
        user_id=user.id,
        provider=provider,
        provider_user_id=provider_user_id,
        email=email,
        display_name=display_name,
        avatar_url=avatar_url,
    )
    db.add(identity)
    db.commit()
    db.refresh(user)
    ensure_admin_role(db, user, provider, provider_user_id)
    return user


def _steam_auth_url(callback_url: str, request: Request, state: str) -> str:
    realm = f"{request.url.scheme}://{request.url.netloc}"
    return_to = _append_query(callback_url, {"state": state})
    params = {
        "openid.ns": "http://specs.openid.net/auth/2.0",
        "openid.mode": "checkid_setup",
        "openid.return_to": return_to,
        "openid.realm": realm,
        "openid.claimed_id": "http://specs.openid.net/auth/2.0/identifier_select",
        "openid.identity": "http://specs.openid.net/auth/2.0/identifier_select",
    }
    return f"{STEAM_OPENID_URL}?{urlencode(params)}"


def _verify_steam_openid(params: dict[str, str]) -> str:
    if params.get("openid.mode") != "id_res":
        raise HTTPException(status_code=400, detail="Invalid Steam OpenID response")

    payload = {key: value for key, value in params.items() if key.startswith("openid.")}
    payload["openid.mode"] = "check_authentication"
    response = requests.post(STEAM_OPENID_URL, data=payload, timeout=10)
    if response.status_code >= 400 or "is_valid:true" not in response.text:
        raise HTTPException(status_code=400, detail="Steam OpenID validation failed")

    claimed_id = params.get("openid.claimed_id") or ""
    steam_id = claimed_id.rstrip("/").split("/")[-1]
    if not steam_id:
        raise HTTPException(status_code=400, detail="Steam ID missing")
    return steam_id


def _fetch_steam_profile(steam_id: str) -> dict[str, Any]:
    if not STEAM_WEB_API_KEY:
        return {}
    try:
        response = requests.get(
            f"{STEAM_WEB_API_URL}/ISteamUser/GetPlayerSummaries/v2/",
            params={"key": STEAM_WEB_API_KEY, "steamids": steam_id},
            timeout=10,
        )
    except RequestException:
        return {}
    if response.status_code >= 400:
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    players = payload.get("response", {}).get("players", [])
    return players[0] if players else {}


def _oauth2_token_exchange(provider: str, code: str, callback_url: str) -> dict[str, Any]:
    if provider == "google":
        token_url = GOOGLE_OAUTH_TOKEN_URL
        data = {
            "code": code,
            "client_id": GOOGLE_OAUTH_CLIENT_ID,
            "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
            "redirect_uri": callback_url,
            "grant_type": "authorization_code",
        }
        try:
            response = requests.post(token_url, data=data, timeout=10)
        except RequestException as exc:
            raise HTTPException(status_code=502, detail="OAuth token exchange failed") from exc
    elif provider == "epic":
        token_url = EPIC_OAUTH_TOKEN_URL
        basic = base64.b64encode(f"{EPIC_OAUTH_CLIENT_ID}:{EPIC_OAUTH_CLIENT_SECRET}".encode()).decode()
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": callback_url,
        }
        try:
            response = requests.post(
                token_url,
                data=data,
                headers={"Authorization": f"Basic {basic}"},
                timeout=10,
            )
        except RequestException as exc:
            raise HTTPException(status_code=502, detail="OAuth token exchange failed") from exc
    elif provider == "discord":
        token_url = DISCORD_OAUTH_TOKEN_URL
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": callback_url,
            "client_id": DISCORD_OAUTH_CLIENT_ID,
            "client_secret": DISCORD_OAUTH_CLIENT_SECRET,
        }
        try:
            response = requests.post(
                token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
        except RequestException as exc:
            raise HTTPException(status_code=502, detail="OAuth token exchange failed") from exc
    else:
        raise HTTPException(status_code=400, detail="Unsupported provider")

    if response.status_code >= 400:
        payload = _safe_json_response(response, "OAuth token exchange")
        error_message = _extract_oauth_error(payload, "OAuth token exchange failed")
        raise HTTPException(status_code=400, detail=error_message)
    return _safe_json_response(response, "OAuth token exchange")


def _oauth2_userinfo(provider: str, access_token: str) -> dict[str, Any]:
    if provider == "google":
        userinfo_url = GOOGLE_OAUTH_USERINFO_URL
    elif provider == "epic":
        userinfo_url = EPIC_OAUTH_USERINFO_URL
    elif provider == "discord":
        userinfo_url = DISCORD_OAUTH_USERINFO_URL
    else:
        raise HTTPException(status_code=400, detail="Unsupported provider")

    try:
        response = requests.get(
            userinfo_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
    except RequestException as exc:
        raise HTTPException(status_code=502, detail="OAuth userinfo request failed") from exc
    if response.status_code >= 400:
        payload = _safe_json_response(response, "OAuth userinfo")
        error_message = _extract_oauth_error(payload, "OAuth userinfo failed")
        raise HTTPException(status_code=400, detail=error_message)
    return _safe_json_response(response, "OAuth userinfo")


@router.post("/register", response_model=UserOut)
def register(user_in: UserCreate, db: Session = Depends(get_db)):
    existing = (
        db.query(User)
        .filter((User.email == user_in.email) | (User.username == user_in.username))
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="User already exists")

    user = User(
        email=user_in.email,
        username=user_in.username,
        display_name=user_in.display_name or user_in.username,
        password_hash=get_password_hash(user_in.password)
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    ensure_admin_role(db, user)
    return user


@router.post("/login", response_model=Token)
def login(user_in: UserLogin, db: Session = Depends(get_db)):
    identifier = (
        user_in.email_or_username
        or (user_in.email if user_in.email else None)
        or user_in.username
    )
    if not identifier:
        raise HTTPException(status_code=400, detail="Missing login identifier")

    user = (
        db.query(User)
        .filter((User.email == identifier) | (User.username == identifier))
        .first()
    )

    if not user or not verify_password(user_in.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    ensure_admin_role(db, user)
    return _issue_tokens(db, user)


@router.post("/refresh", response_model=Token)
def refresh(token_in: TokenRefresh, db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token_in.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        token_type = payload.get("type")
        if token_type != "refresh":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")
        user_id: str | None = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token subject")
    except JWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    ensure_admin_role(db, user)
    access = create_access_token(user.id)
    refresh = create_refresh_token(user.id)
    cache_client.set_session(user.id, access, SESSION_TTL_SECONDS)
    return {"access_token": access, "refresh_token": refresh, "token_type": "bearer", "user": user}


@router.get("/oauth/providers", response_model=list[OAuthProviderOut])
def oauth_providers():
    return [
        {"provider": provider, "label": label, "enabled": _provider_enabled(provider)}
        for provider, label in OAUTH_PROVIDER_LABELS.items()
    ]


@router.get("/oauth/{provider}/start")
def oauth_start(
    provider: str,
    request: Request,
    redirect_uri: str | None = None,
    request_id: str | None = None,
    debug: bool = False,
):
    provider = provider.lower()
    print(f"[OAuth DEBUG] /start called for provider={provider}, redirect_uri={redirect_uri}")
    if provider not in OAUTH_PROVIDER_LABELS:
        raise HTTPException(status_code=404, detail="Provider not supported")
    if not _provider_enabled(provider):
        raise HTTPException(status_code=400, detail="Provider not configured")

    target = redirect_uri or f"{FRONTEND_BASE_URL}/oauth/callback"
    if not _safe_redirect(target):
        raise HTTPException(status_code=400, detail="Invalid redirect_uri")

    state = secrets.token_urlsafe(32)
    state_key = _oauth_state_key(state)
    print(f"[OAuth DEBUG] Storing state: key={state_key}, state={state[:16]}...")
    
    state_data = {
        "provider": provider,
        "redirect_uri": target,
    }
    if request_id:
         state_data["request_id"] = request_id

    cache_client.set_oauth_state(
        state_key,
        state_data,
        ttl=OAUTH_STATE_TTL_SECONDS,
    )
    # Verify state was stored
    verify = cache_client.get_oauth_state(state_key)
    print(f"[OAuth DEBUG] State stored verify: {verify is not None}")

    callback_url = _build_callback_url(request, provider)
    if provider == "steam":
        auth_url = _steam_auth_url(callback_url, request, state)
    elif provider == "google":
        params = {
            "client_id": GOOGLE_OAUTH_CLIENT_ID,
            "redirect_uri": callback_url,
            "response_type": "code",
            "scope": GOOGLE_OAUTH_SCOPES,
            "state": state,
            "access_type": "offline",
            "prompt": "select_account",
        }
        auth_url = f"{GOOGLE_OAUTH_AUTH_URL}?{urlencode(params)}"
    elif provider == "epic":
        params = {
            "client_id": EPIC_OAUTH_CLIENT_ID,
            "redirect_uri": callback_url,
            "response_type": "code",
            "scope": EPIC_OAUTH_SCOPES,
            "state": state,
            "prompt": "login",
        }
        auth_url = f"{EPIC_OAUTH_AUTH_URL}?{urlencode(params)}"
    elif provider == "discord":
        params = {
            "client_id": DISCORD_OAUTH_CLIENT_ID,
            "redirect_uri": callback_url,
            "response_type": "code",
            "scope": DISCORD_OAUTH_SCOPES,
            "state": state,
        }
        auth_url = f"{DISCORD_OAUTH_AUTH_URL}?{urlencode(params)}"
    else:
        raise HTTPException(status_code=404, detail="Provider not supported")

    print(f"[OAuth DEBUG] Generated auth_url for {provider}: {auth_url[:200]}...")
    print(f"[OAuth DEBUG] State parameter: {state}")

    if debug:
        return {
            "provider": provider,
            "auth_url": auth_url,
            "callback_url": callback_url,
            "redirect_target": target,
            "state": state,
        }
    return RedirectResponse(auth_url)


@router.get("/oauth/{provider}/callback", name="oauth_callback")
def oauth_callback(
    provider: str,
    request: Request,
    state: str | None = None,
    code: str | None = None,
    db: Session = Depends(get_db),
):
    provider = provider.lower()
    
    # Handle missing state parameter
    if not state:
        print(f"[OAuth DEBUG] /callback called for provider={provider} with NO STATE parameter")
        print(f"[OAuth DEBUG] Query params: {dict(request.query_params)}")
        raise HTTPException(
            status_code=400, 
            detail="Missing OAuth state parameter. Please restart the authentication process."
        )
    
    state_key = _oauth_state_key(state)
    print(f"[OAuth DEBUG] /callback called for provider={provider}, state={state[:16]}...")
    print(f"[OAuth DEBUG] Looking up key={state_key}")
    try:
        state_payload = cache_client.get_oauth_state(state_key)
        print(f"[OAuth DEBUG] State lookup result: {state_payload}")
        if not state_payload or state_payload.get("provider") != provider:
            print(f"[OAuth DEBUG] State validation FAILED: payload={state_payload}, expected_provider={provider}")
            raise HTTPException(status_code=400, detail="Invalid OAuth state")
        print(f"[OAuth DEBUG] State validation OK, deleting state")
        cache_client.delete_oauth_state(state_key)

        redirect_target = state_payload.get("redirect_uri") or f"{FRONTEND_BASE_URL}/oauth/callback"
        if not _safe_redirect(redirect_target):
            raise HTTPException(status_code=400, detail="Invalid redirect_uri")

        if provider == "steam":
            params = dict(request.query_params)
            steam_id = _verify_steam_openid(params)
            profile = _fetch_steam_profile(steam_id)
            oauth_user = {
                "provider": "steam",
                "provider_user_id": steam_id,
                "email": None,
                "display_name": profile.get("personaname") or f"SteamUser{steam_id[-4:]}",
                "avatar_url": profile.get("avatarfull"),
            }
        else:
            if not code:
                raise HTTPException(status_code=400, detail="Missing OAuth code")
            callback_url = _build_callback_url(request, provider)
            token_payload = _oauth2_token_exchange(provider, code, callback_url)
            access_token = token_payload.get("access_token")
            if not access_token:
                raise HTTPException(status_code=400, detail="Missing access token")
            userinfo = _oauth2_userinfo(provider, access_token)
            if provider == "google":
                oauth_user = {
                    "provider": "google",
                    "provider_user_id": userinfo.get("sub") or "",
                    "email": userinfo.get("email"),
                    "display_name": userinfo.get("name") or userinfo.get("email"),
                    "avatar_url": userinfo.get("picture"),
                }
            elif provider == "discord":
                oauth_user = {
                    "provider": "discord",
                    "provider_user_id": userinfo.get("id") or "",
                    "email": userinfo.get("email"),
                    "display_name": userinfo.get("username") or userinfo.get("global_name"),
                    "avatar_url": f"https://cdn.discordapp.com/avatars/{userinfo.get('id')}/{userinfo.get('avatar')}.png" if userinfo.get("avatar") else None,
                }
            else:
                oauth_user = {
                    "provider": "epic",
                    "provider_user_id": userinfo.get("account_id") or userinfo.get("sub") or "",
                    "email": userinfo.get("email"),
                    "display_name": userinfo.get("displayName") or userinfo.get("name"),
                    "avatar_url": userinfo.get("avatar"),
                }

            if not oauth_user["provider_user_id"]:
                raise HTTPException(status_code=400, detail="OAuth user ID missing")

        user = _get_or_create_user(db, oauth_user)
        exchange_code = secrets.token_urlsafe(32)
        cache_client.set_json(
            _oauth_exchange_key(exchange_code),
            {"user_id": user.id},
            ttl=OAUTH_STATE_TTL_SECONDS,
        )

        request_id = state_payload.get("request_id")
        if request_id:
            # Poll mode: store the code in a poll cache
            poll_key = _oauth_poll_key(request_id)
            cache_client.set_json(
                 poll_key,
                 {"code": exchange_code},
                 ttl=OAUTH_STATE_TTL_SECONDS
            )
            # Return a simple success page
            return _render_success_page()

        redirect_url = _append_query(
            redirect_target, {"code": exchange_code, "provider": provider}
        )
        return RedirectResponse(redirect_url)
    except HTTPException:
        raise
    except Exception as exc:
        _log_oauth_error(provider, request, exc)
        detail = "OAuth callback failed"
        if OAUTH_DEBUG_ERRORS:
            detail = f"{detail}: {exc}"
        raise HTTPException(status_code=500, detail=detail) from exc

def _render_success_page():
     html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Login Successful</title>
        <style>
            body { font-family: sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; background: #0f172a; color: #fff; }
            .container { text-align: center; background: #1e293b; padding: 2rem; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
            h1 { color: #4ade80; margin-bottom: 1rem; }
            p { color: #94a3b8; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Login Successful</h1>
            <p>You can close this window now.</p>
            <script>
                // Try to close automatically
                setTimeout(() => window.close(), 2000);
            </script>
        </div>
    </body>
    </html>
    """
     return fastapi.responses.HTMLResponse(content=html)


@router.get("/oauth/poll/{request_id}")
def oauth_poll(request_id: str):
    """Callback polling endpoint for Portable launchers"""
    poll_key = _oauth_poll_key(request_id)
    data = cache_client.get_json(poll_key)
    if not data:
        # Not ready yet, 204 or 404? 202 Accepted = pending
        # Return 204 No Content to indicate pending but not found yet
        return fastapi.responses.Response(status_code=204)
    
    # Found it!
    # Ensure we return json
    return {"code": data.get("code")}


@router.post("/oauth/exchange", response_model=Token)
def oauth_exchange(payload: OAuthExchangeIn, db: Session = Depends(get_db)):
    data = cache_client.get_json(_oauth_exchange_key(payload.code))
    if not data:
        raise HTTPException(status_code=400, detail="Invalid or expired code")
    cache_client.delete(_oauth_exchange_key(payload.code))

    user = db.query(User).filter(User.id == data.get("user_id")).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    ensure_admin_role(db, user)
    return _issue_tokens(db, user)


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)):
    return current_user


@router.post("/logout")
def logout(current_user: User = Depends(get_current_user)):
    cache_client.delete_session(current_user.id)
    return {"status": "signed_out"}
