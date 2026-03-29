"""
JWT validation (via JWKS) and OPA policy client.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

import httpx
import jwt
from jwt import PyJWKClient

JWKS_URI = os.getenv("JWKS_URI", "http://oauth-service:8081/.well-known/jwks.json")
OPA_URL  = os.getenv("OPA_URL",  "http://opa:8181")

_jwks = PyJWKClient(JWKS_URI, cache_keys=True)


class JWTError(Exception):
    pass


def validate_token(token: str) -> dict:
    """Validate RS256 JWT via JWKS. Returns claims dict."""
    try:
        key = _jwks.get_signing_key_from_jwt(token)
        return jwt.decode(token, key.key, algorithms=["RS256"],
                          options={"verify_aud": False})
    except Exception as exc:
        raise JWTError(str(exc)) from exc


@dataclass
class PolicyDecision:
    allow: bool
    excluded_columns: list[str]       = field(default_factory=list)
    row_filter:       str | None      = None
    column_masks:     dict[str, str]  = field(default_factory=dict)


async def get_policy(principal: str, namespace: str,
                     table: str, operation: str) -> PolicyDecision:
    """
    Query OPA for a policy decision.

    POST /v1/data/iceberg/policy
    Input:  { principal, namespace, table, operation }
    Output: { allow, excluded_columns, row_filter, column_masks }
    """
    async with httpx.AsyncClient(base_url=OPA_URL, timeout=5) as c:
        r = await c.post("/v1/data/iceberg/policy", json={
            "input": {
                "principal": principal,
                "namespace": namespace,
                "table":     table,
                "operation": operation,
            }
        })
        r.raise_for_status()
        result = r.json().get("result", {})

    return PolicyDecision(
        allow=bool(result.get("allow", False)),
        excluded_columns=result.get("excluded_columns") or [],
        row_filter=result.get("row_filter"),
        column_masks=result.get("column_masks") or {},
    )
