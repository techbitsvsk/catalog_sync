"""OAuth 2.0 client credentials token manager with automatic refresh."""
from __future__ import annotations

import logging
import time
from typing import Optional

import requests

log = logging.getLogger(__name__)


class OAuthClient:
    """
    OAuth 2.0 client credentials token manager.

    Fetches access tokens from an OAuth server using the client_credentials
    grant and caches them until 30 seconds before expiry.

    Args:
        server_url:    Base URL of the OAuth service (e.g. http://localhost:8081)
        client_id:     OAuth client ID
        client_secret: OAuth client secret
        scope:         Space-separated scopes to request

    Usage:
        client = OAuthClient("http://localhost:8081", "sync-service", "sync-secret",
                             scope="catalog:read catalog:write")
        token = client.get_token()   # Cached; auto-refreshes before expiry
    """

    def __init__(
        self,
        server_url: str,
        client_id: str,
        client_secret: str,
        scope: str = "catalog:read catalog:write",
    ):
        self._url = server_url.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._cached_token: Optional[str] = None
        self._expires_at: float = 0.0

    @property
    def client_id(self) -> str:
        return self._client_id

    def get_token(self) -> str:
        """Return a valid access token, fetching a new one if expired or absent."""
        if self._cached_token and time.monotonic() < self._expires_at - 30:
            return self._cached_token
        return self._fetch_token()

    def _fetch_token(self) -> str:
        resp = requests.post(
            f"{self._url}/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": self._scope,
            },
            timeout=10,
        )
        if not resp.ok:
            raise RuntimeError(
                f"OAuth token request failed ({resp.status_code}): {resp.text}"
            )
        data = resp.json()
        self._cached_token = data["access_token"]
        expires_in = data.get("expires_in", 3600)
        self._expires_at = time.monotonic() + expires_in
        log.debug(f"Fetched new OAuth token for {self._client_id} (expires in {expires_in}s)")
        return self._cached_token

    def invalidate(self) -> None:
        """Force token refresh on next get_token() call."""
        self._cached_token = None
        self._expires_at = 0.0
