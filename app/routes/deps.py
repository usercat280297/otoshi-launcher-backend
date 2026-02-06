from typing import Optional
from fastapi import Depends, HTTPException, status, Header
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from sqlalchemy.orm import Session

from ..core.config import SECRET_KEY, ALGORITHM, ADMIN_API_KEY
from ..core.cache import cache_client
from ..db import get_db
from ..models import User
from ..utils.admin import is_admin_identity

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
oauth2_scheme_optional = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> User:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        token_type = payload.get("type")
        if token_type not in (None, "access"):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    except JWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc

    session_token = cache_client.get_session(user_id)
    if session_token and session_token != token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


def get_current_user_optional(
    token: str = Depends(oauth2_scheme_optional),
    db: Session = Depends(get_db)
) -> Optional[User]:
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        token_type = payload.get("type")
        if token_type not in (None, "access"):
            return None
        user_id: str = payload.get("sub")
        if user_id is None:
            return None
    except JWTError:
        return None
    session_token = cache_client.get_session(user_id)
    if session_token and session_token != token:
        return None

    return db.query(User).filter(User.id == user_id).first()


def require_admin_user(current_user: User = Depends(get_current_user)) -> User:
    if not is_admin_identity(current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return current_user


def require_admin_access(
    x_api_key: Optional[str] = Header(None),
    current_user: Optional[User] = Depends(get_current_user_optional),
) -> Optional[User]:
    if ADMIN_API_KEY and x_api_key == ADMIN_API_KEY:
        return current_user
    if current_user and is_admin_identity(current_user):
        return current_user
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
