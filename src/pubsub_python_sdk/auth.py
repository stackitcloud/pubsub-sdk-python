"""Authentication integration using stackit.authorization."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthConfig:
    """Configuration for STACKIT authorization.

    Attributes:
        service_account_key: Path to or content of the service account key JSON.
        token_endpoint: OAuth2 token endpoint URL.
        client_id: OAuth2 client ID.
        client_secret: OAuth2 client secret.
        scope: OAuth2 scope(s) to request.
    """

    service_account_key: str | None = None
    token_endpoint: str | None = "https://authorization.stackit.cloud/auth/token"
    client_id: str | None = None
    client_secret: str | None = None
    scope: str = "dataplane"


class StackitAuthProvider:
    """Handles authentication with STACKIT using the stackit.authorization library.

    This provider manages token acquisition and refresh, providing an access token
    that can be included in API requests.

    Attributes:
        config: The authentication configuration.
        _token: The current access token (cached until expiry).
        _expires_at: Unix timestamp when the current token expires.
    """

    def __init__(self, config: AuthConfig):
        self.config = config
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._tokens: list[dict[str, Any]] = []

    def _get_token_from_stackit(self) -> tuple[str, float]:
        """Fetch a token from STACKIT authorization service.

        Uses the stackit-core library to obtain an OAuth2 access token.
        Falls back to cached credentials if the library is not available.

        Returns:
            Tuple of (access_token, expires_in_seconds).

        Raises:
            PubSubAuthenticationError: If authentication fails.
        """
        try:
            import json as json_lib
            from datetime import datetime

            from stackit.core.auth_methods.key_auth import KeyAuth
            from stackit.core.authorization import (
                Authorization,
                Configuration,
                ServiceAccountKey,
            )

            # Patch KeyAuth to handle missing refresh_token
            if not hasattr(KeyAuth, "_KeyAuth__patched"):

                def _patched_fetch(self: KeyAuth) -> None:
                    body = {
                        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                        "assertion": self.initial_token,
                    }
                    try:
                        import requests

                        response = requests.post(
                            self.token_endpoint, data=body, timeout=self.timeout
                        )
                        response.raise_for_status()
                        response_json = response.json()
                        self.access_token = response_json["access_token"]
                        self.refresh_token = response_json.get("refresh_token", "")
                    except requests.RequestException as e:
                        raise requests.RequestException("Initial token fetch failed") from e

                KeyAuth._KeyAuth__fetch_token_from_endpoint = _patched_fetch
                KeyAuth._KeyAuth__patched = True

            sa_key_content = self.config.service_account_key
            sa_key_stripped = sa_key_content.strip() if sa_key_content else ""

            if sa_key_stripped.startswith("{"):
                sa_key_data = json_lib.loads(sa_key_stripped)
            elif sa_key_stripped:
                with open(sa_key_stripped) as f:
                    sa_key_data = json_lib.loads(f.read())
            else:
                raise ValueError("No service account key configured")

            cred = sa_key_data.get("credentials", {})

            credentials_dict = {
                "active": sa_key_data.get("active", True),
                "createdAt": datetime.fromisoformat(
                    sa_key_data["createdAt"].replace("Z", "+00:00")
                ),
                "credentials": {
                    "aud": cred.get("aud"),
                    "iss": cred.get("iss"),
                    "kid": cred.get("kid"),
                    "sub": cred.get("sub"),
                    "privateKey": cred.get("privateKey"),
                    "tokenEndpoint": cred.get("tokenEndpoint"),
                },
                "id": sa_key_data["id"],
                "keyAlgorithm": sa_key_data.get("keyAlgorithm"),
                "keyOrigin": sa_key_data.get("keyOrigin"),
                "keyType": sa_key_data.get("keyType"),
                "publicKey": sa_key_data.get("publicKey"),
            }

            credentials = ServiceAccountKey.model_validate(credentials_dict)

            config = Configuration(
                service_account_key=credentials,
                custom_token_endpoint=cred.get("tokenEndpoint", self.config.token_endpoint),
            )

            auth = Authorization(config)
            auth.service_account_key = credentials
            auth.auth_method = auth._Authorization__get_authentication()

            token = auth.auth_method.access_token
            return token, 3600

        except ImportError:
            logger.warning(
                "stackit-core not installed; using fallback token provider. "
                "Install the package for full authentication support."
            )
            return self._get_fallback_token()
        except Exception as exc:
            from pubsub_python_sdk.errors import PubSubAuthenticationError

            raise PubSubAuthenticationError(f"Failed to obtain STACKIT token: {exc}") from exc

    def _get_fallback_token(self) -> tuple[str, float]:
        """Fallback token acquisition when stackit.authorization is unavailable.

        Returns:
            Tuple of (access_token, expires_in_seconds).
        """
        # If we have credentials but the library isn't available, return a placeholder
        # In production this should never be hit if the library is installed
        if self.config.client_id and self.config.client_secret:
            return f"Bearer {self.config.client_id}:{self.config.client_secret}", 3600
        from pubsub_python_sdk.errors import PubSubAuthenticationError

        raise PubSubAuthenticationError(
            "No credentials configured and stackit.authorization unavailable"
        )

    def get_token(self) -> str:
        """Get a valid access token, refreshing if necessary.

        Returns:
            The access token string suitable for the Authorization header.
        """
        import time as _time

        # Check if we have a cached valid token
        if self._token and _time.time() < self._expires_at - 60:  # 60s buffer
            return self._token

        # Fetch a new token
        token, expires_in = self._get_token_from_stackit()
        self._token = token
        self._expires_at = _time.time() + expires_in
        self._tokens.append(
            {
                "token": token,
                "expires_at": self._expires_at,
                "issued_at": _time.time(),
            }
        )
        return token

    def get_auth_header(self) -> dict[str, str]:
        """Get the Authorization header value.

        Returns:
            Dictionary with the Authorization header key and token value.
        """
        return {"Authorization": self.get_token()}


__all__ = [
    "AuthConfig",
    "StackitAuthProvider",
]
