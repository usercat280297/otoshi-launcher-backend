import hashlib
from datetime import datetime, timedelta
from jose import jwt
from passlib.context import CryptContext

from .config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def _normalize_password(password: str) -> str:
    raw = password.encode("utf-8")
    if len(raw) <= 72:
        return password
    digest = hashlib.sha256(raw).hexdigest()
    return f"sha256${digest}"

def get_password_hash(password: str) -> str:
    return pwd_context.hash(_normalize_password(password))


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(_normalize_password(plain_password), hashed_password)


def _create_token(subject: str, token_type: str, expires_delta: timedelta) -> str:
    expire = datetime.utcnow() + expires_delta
    to_encode = {"sub": subject, "exp": expire, "type": token_type}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(subject: str) -> str:
    return _create_token(subject, "access", timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))


def create_refresh_token(subject: str) -> str:
    return _create_token(subject, "refresh", timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS))
