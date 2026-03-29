"""
oauth_service/main.py — Simple OAuth 2.0 Authorization Server

Supports:
  - client_credentials grant (RFC 6749 §4.4)
  - RS256 JWT access tokens
  - OIDC discovery (/.well-known/openid-configuration)
  - JWKS endpoint for JWT validation by resource servers (Nessie)
  - Token introspection (RFC 7662)
  - Client management API (admin-only)

Default clients (seeded on startup):
  admin-client     / admin-secret      — scopes: catalog:read catalog:write catalog:admin catalog:drop
  analytics-client / analytics-secret  — scopes: catalog:read
  sync-service     / sync-secret       — scopes: catalog:read catalog:write
  data-scientist   / ds-secret         — scopes: catalog:read
"""

import base64
import hashlib
import logging
from contextlib import asynccontextmanager
from typing import Annotated

import bcrypt
from fastapi import FastAPI, HTTPException, Depends, Form, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config import settings
from crypto import (
    generate_keys, set_keys_from_pem, get_private_pem,
    get_jwks, create_access_token, decode_token,
)
from models import Base, OAuthClient, KeyStore

log = logging.getLogger(__name__)

def _hash_secret(secret: str) -> str:
    """SHA-256 pre-hash avoids bcrypt's 72-byte truncation, then bcrypt for storage."""
    digest = base64.b64encode(hashlib.sha256(secret.encode()).digest())
    return bcrypt.hashpw(digest, bcrypt.gensalt()).decode()


def _verify_secret(secret: str, hashed: str) -> bool:
    try:
        digest = base64.b64encode(hashlib.sha256(secret.encode()).digest())
        return bcrypt.checkpw(digest, hashed.encode())
    except Exception:
        return False


engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ── Startup ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables
    Base.metadata.create_all(bind=engine)

    # Load or generate RSA keys
    with SessionLocal() as db:
        key_row = db.query(KeyStore).filter_by(key_id=settings.jwks_kid).first()
        if key_row:
            set_keys_from_pem(key_row.private_pem.encode())
            log.info("Loaded RSA keys from database")
        else:
            generate_keys()
            db.add(KeyStore(key_id=settings.jwks_kid, private_pem=get_private_pem().decode()))
            db.commit()
            log.info("Generated and stored new RSA key pair")

        # Seed default clients
        _seed_clients(db)

    yield


def _seed_clients(db: Session):
    defaults = [
        ("admin-client", "admin-secret", "Admin Client",
         "catalog:read catalog:write catalog:admin catalog:drop"),
        ("analytics-client", "analytics-secret", "Analytics Client",
         "catalog:read"),
        ("sync-service", "sync-secret", "Sync Service",
         "catalog:read catalog:write"),
        ("data-scientist", "ds-secret", "Data Scientist",
         "catalog:read"),
    ]
    for client_id, secret, name, scopes in defaults:
        existing = db.query(OAuthClient).filter_by(client_id=client_id).first()
        if not existing:
            db.add(OAuthClient(
                client_id=client_id,
                client_secret_hash=_hash_secret(secret),
                name=name,
                scopes=scopes,
            ))
            log.info(f"Seeded client: {client_id}")
    db.commit()


app = FastAPI(title="Catalog OAuth Service", version="1.0.0", lifespan=lifespan)
security = HTTPBearer(auto_error=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def require_admin(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]):
    if not credentials or credentials.credentials != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")


# ── OIDC Discovery ─────────────────────────────────────────────────────────

@app.get("/.well-known/openid-configuration")
def openid_configuration(request: Request):
    base = settings.issuer
    return {
        "issuer": base,
        "token_endpoint": f"{base}/token",
        "jwks_uri": f"{base}/.well-known/jwks.json",
        "introspection_endpoint": f"{base}/introspect",
        "grant_types_supported": ["client_credentials"],
        "token_endpoint_auth_methods_supported": ["client_secret_post"],
        "response_types_supported": ["token"],
        "scopes_supported": ["catalog:read", "catalog:write", "catalog:admin", "catalog:drop"],
    }


@app.get("/.well-known/jwks.json")
def jwks_endpoint():
    return get_jwks()


# ── Token Endpoint ─────────────────────────────────────────────────────────

@app.post("/token")
def token_endpoint(
    grant_type: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    scope: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if grant_type != "client_credentials":
        raise HTTPException(status_code=400, detail="unsupported_grant_type")

    client = db.query(OAuthClient).filter_by(client_id=client_id, is_active=True).first()
    if not client or not _verify_secret(client_secret, client.client_secret_hash):
        raise HTTPException(status_code=401, detail="invalid_client")

    # Intersect requested scopes with allowed scopes
    allowed = set(client.scopes.split())
    requested = set(scope.split()) if scope else allowed
    granted = list(requested & allowed) if requested else list(allowed)

    if not granted:
        raise HTTPException(status_code=400, detail="invalid_scope")

    token, expires_in = create_access_token(client_id=client_id, scopes=granted)

    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "scope": " ".join(granted),
        "client_id": client_id,
    }


# ── Token Introspection (RFC 7662) ─────────────────────────────────────────

@app.post("/introspect")
def introspect(
    token: str = Form(...),
    db: Session = Depends(get_db),
):
    try:
        claims = decode_token(token)
        client_id = claims.get("client_id") or claims.get("sub")
        client = db.query(OAuthClient).filter_by(client_id=client_id, is_active=True).first()
        if not client:
            return {"active": False}
        return {
            "active": True,
            "client_id": client_id,
            "scope": claims.get("scope", ""),
            "sub": claims.get("sub"),
            "iss": claims.get("iss"),
            "exp": claims.get("exp"),
            "iat": claims.get("iat"),
        }
    except Exception:
        return {"active": False}


# ── Client Management ──────────────────────────────────────────────────────

class ClientCreate(BaseModel):
    client_id: str
    client_secret: str
    name: str
    description: str = ""
    scopes: str = "catalog:read"


@app.post("/clients", dependencies=[Depends(require_admin)], status_code=201)
def create_client(body: ClientCreate, db: Session = Depends(get_db)):
    existing = db.query(OAuthClient).filter_by(client_id=body.client_id).first()
    if existing:
        raise HTTPException(status_code=409, detail="Client already exists")
    client = OAuthClient(
        client_id=body.client_id,
        client_secret_hash=_hash_secret(body.client_secret),
        name=body.name,
        description=body.description,
        scopes=body.scopes,
    )
    db.add(client)
    db.commit()
    return {"client_id": body.client_id, "name": body.name, "scopes": body.scopes}


@app.get("/clients", dependencies=[Depends(require_admin)])
def list_clients(db: Session = Depends(get_db)):
    clients = db.query(OAuthClient).all()
    return [
        {
            "client_id": c.client_id,
            "name": c.name,
            "description": c.description,
            "scopes": c.scopes,
            "is_active": c.is_active,
            "created_at": c.created_at.isoformat() if c.created_at else None,
        }
        for c in clients
    ]


@app.delete("/clients/{client_id}", dependencies=[Depends(require_admin)])
def deactivate_client(client_id: str, db: Session = Depends(get_db)):
    client = db.query(OAuthClient).filter_by(client_id=client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    client.is_active = False
    db.commit()
    return {"client_id": client_id, "status": "deactivated"}


@app.get("/health")
def health():
    return {"status": "ok", "service": "oauth"}
