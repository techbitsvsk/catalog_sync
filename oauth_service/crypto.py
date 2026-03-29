"""RSA key management and JWT operations."""
import time
import uuid
from base64 import urlsafe_b64encode
from typing import Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwt

from config import settings

# Module-level key storage (generated once on startup, cached in DB)
_private_key = None
_public_key = None
_jwks_cache: Optional[dict] = None


def generate_keys():
    """Generate RSA key pair. Called on startup if no keys exist."""
    global _private_key, _public_key
    _private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=settings.rsa_key_bits,
    )
    _public_key = _private_key.public_key()


def set_keys_from_pem(private_pem: bytes):
    """Load existing keys from PEM bytes stored in DB."""
    global _private_key, _public_key
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    _private_key = load_pem_private_key(private_pem, password=None)
    _public_key = _private_key.public_key()


def get_private_pem() -> bytes:
    return _private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )


def get_public_pem() -> bytes:
    return _public_key.public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _int_to_bytes(n: int) -> bytes:
    length = (n.bit_length() + 7) // 8
    return n.to_bytes(length, "big")


def get_jwks() -> dict:
    """Return JWKS document with the public key."""
    global _jwks_cache
    if _jwks_cache:
        return _jwks_cache
    pub_numbers_obj = _public_key.public_numbers()
    n = pub_numbers_obj.n
    e = pub_numbers_obj.e

    def b64url(num: int) -> str:
        b = _int_to_bytes(num)
        return urlsafe_b64encode(b).rstrip(b"=").decode()

    _jwks_cache = {
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": settings.jwks_kid,
            "n": b64url(n),
            "e": b64url(e),
        }]
    }
    return _jwks_cache


def create_access_token(
    client_id: str,
    scopes: list[str],
    expires_in: int = None,
) -> tuple[str, int]:
    """Create a signed JWT. Returns (token_string, expires_in_seconds)."""
    if expires_in is None:
        expires_in = settings.access_token_expire_seconds
    now = int(time.time())
    payload = {
        "iss": settings.issuer,
        "sub": client_id,
        "aud": ["nessie-server"],
        "iat": now,
        "exp": now + expires_in,
        "jti": str(uuid.uuid4()),
        "client_id": client_id,
        "scope": " ".join(scopes),
    }
    private_pem = get_private_pem()
    token = jwt.encode(
        payload,
        private_pem.decode(),
        algorithm="RS256",
        headers={"kid": settings.jwks_kid},
    )
    return token, expires_in


def decode_token(token: str) -> dict:
    """Decode and verify a JWT using the local public key."""
    public_pem = get_public_pem()
    return jwt.decode(
        token,
        public_pem.decode(),
        algorithms=["RS256"],
        audience="nessie-server",
        issuer=settings.issuer,
    )
