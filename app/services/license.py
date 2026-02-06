import base64
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import uuid

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from ..models import License

BASE_DIR = Path(__file__).resolve().parents[2]
KEY_DIR = BASE_DIR / "keys"
PRIVATE_KEY_PATH = KEY_DIR / "license_private.pem"
PUBLIC_KEY_PATH = KEY_DIR / "license_public.pem"


def _load_or_generate_keys() -> tuple[bytes, bytes]:
    env_private = os.getenv("LICENSE_PRIVATE_KEY")
    env_public = os.getenv("LICENSE_PUBLIC_KEY")
    if env_private and env_public:
        return env_private.encode("utf-8"), env_public.encode("utf-8")

    if PRIVATE_KEY_PATH.exists() and PUBLIC_KEY_PATH.exists():
        return PRIVATE_KEY_PATH.read_bytes(), PUBLIC_KEY_PATH.read_bytes()

    KEY_DIR.mkdir(parents=True, exist_ok=True)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    PRIVATE_KEY_PATH.write_bytes(private_pem)
    PUBLIC_KEY_PATH.write_bytes(public_pem)
    return private_pem, public_pem


def get_public_key_pem() -> str:
    _, public_pem = _load_or_generate_keys()
    return public_pem.decode("ascii")


def build_signing_payload(license: License) -> str:
    return "|".join(
        [
            license.id,
            license.user_id,
            license.game_id,
            license.issued_at.replace(tzinfo=timezone.utc).isoformat(),
            license.expires_at.replace(tzinfo=timezone.utc).isoformat()
            if license.expires_at
            else "",
            str(license.max_activations),
            str(license.current_activations),
            license.hardware_id or "",
        ]
    )


def sign_license(license: License) -> str:
    private_pem, _ = _load_or_generate_keys()
    private_key = serialization.load_pem_private_key(private_pem, password=None)
    payload = build_signing_payload(license).encode("utf-8")
    signature = private_key.sign(payload, padding.PKCS1v15(), hashes.SHA256())
    return base64.b64encode(signature).decode("ascii")


def issue_license(
    user_id: str,
    game_id: str,
    hardware_id: Optional[str],
    expires_at: Optional[datetime],
    max_activations: int,
) -> License:
    license_key = uuid.uuid4().hex
    license_item = License(
        id=str(uuid.uuid4()),
        user_id=user_id,
        game_id=game_id,
        license_key=license_key,
        hardware_id=hardware_id,
        issued_at=datetime.utcnow(),
        expires_at=expires_at,
        max_activations=max_activations,
        current_activations=0,
        status="active",
    )
    license_item.signature = sign_license(license_item)
    return license_item
