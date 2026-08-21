"""Tests for the auth module."""

from __future__ import annotations

import json
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

from pubsub_python_sdk.auth import AuthConfig, StackitAuthProvider
from pubsub_python_sdk.errors import PubSubAuthenticationError

SAMPLE_SA_KEY = {
    "active": True,
    "createdAt": "2024-01-01T00:00:00Z",
    "credentials": {
        "aud": "https://stackit.cloud",
        "iss": "sa-issuer",
        "kid": "key-id",
        "sub": "sa-subject",
        "privateKey": "fake-private-key",
        "tokenEndpoint": "https://authorization.stackit.cloud/auth/token",
    },
    "id": "sa-key-id",
    "keyAlgorithm": "RSA",
    "keyOrigin": "USER_PROVIDED",
    "keyType": "USER_MANAGED",
    "publicKey": "fake-public-key",
}


def _mock_authorization_cls(access_token: str) -> MagicMock:
    """Build a mock standing in for stackit.core.authorization.Authorization."""
    mock_auth_method = MagicMock()
    mock_auth_method.access_token = access_token
    mock_instance = MagicMock()
    mock_instance._Authorization__get_authentication.return_value = mock_auth_method
    return MagicMock(return_value=mock_instance)


class TestGetTokenFromStackit:
    """Test StackitAuthProvider._get_token_from_stackit."""

    def test_success_with_inline_json(self):
        config = AuthConfig(service_account_key=json.dumps(SAMPLE_SA_KEY))
        provider = StackitAuthProvider(config)

        with patch(
            "stackit.core.authorization.Authorization",
            _mock_authorization_cls("token-abc"),
        ):
            token, expires_in = provider._get_token_from_stackit()

        assert token == "token-abc"
        assert expires_in == 3600

    def test_success_with_key_file_path(self, tmp_path):
        key_path = tmp_path / "sa-key.json"
        key_path.write_text(json.dumps(SAMPLE_SA_KEY))
        config = AuthConfig(service_account_key=str(key_path))
        provider = StackitAuthProvider(config)

        with patch(
            "stackit.core.authorization.Authorization",
            _mock_authorization_cls("token-file"),
        ):
            token, expires_in = provider._get_token_from_stackit()

        assert token == "token-file"
        assert expires_in == 3600

    def test_empty_key_raises_auth_error(self):
        config = AuthConfig(service_account_key="   ")
        provider = StackitAuthProvider(config)

        with pytest.raises(PubSubAuthenticationError, match="Failed to obtain STACKIT token"):
            provider._get_token_from_stackit()

    def test_no_key_raises_auth_error(self):
        config = AuthConfig(service_account_key=None)
        provider = StackitAuthProvider(config)

        with pytest.raises(PubSubAuthenticationError):
            provider._get_token_from_stackit()

    def test_invalid_json_raises_auth_error(self):
        config = AuthConfig(service_account_key="{not valid json")
        provider = StackitAuthProvider(config)

        with pytest.raises(PubSubAuthenticationError):
            provider._get_token_from_stackit()

    def test_malformed_key_raises_auth_error(self):
        bad_key = dict(SAMPLE_SA_KEY)
        del bad_key["id"]
        config = AuthConfig(service_account_key=json.dumps(bad_key))
        provider = StackitAuthProvider(config)

        with pytest.raises(PubSubAuthenticationError):
            provider._get_token_from_stackit()

    def test_import_error_falls_back_to_fallback_token(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "stackit.core.auth_methods.key_auth", None)
        config = AuthConfig(
            service_account_key=json.dumps(SAMPLE_SA_KEY),
            client_id="fallback-id",
            client_secret="fallback-secret",
        )
        provider = StackitAuthProvider(config)

        token, expires_in = provider._get_token_from_stackit()

        assert token == "fallback-id:fallback-secret"
        assert expires_in == 3600


class TestFallbackToken:
    """Test StackitAuthProvider._get_fallback_token."""

    def test_fallback_with_credentials(self):
        config = AuthConfig(client_id="my-id", client_secret="my-secret")
        provider = StackitAuthProvider(config)

        token, expires_in = provider._get_fallback_token()

        assert token == "my-id:my-secret"
        assert expires_in == 3600

    def test_fallback_without_credentials_raises(self):
        config = AuthConfig(client_id=None, client_secret=None)
        provider = StackitAuthProvider(config)

        with pytest.raises(PubSubAuthenticationError, match="No credentials configured"):
            provider._get_fallback_token()


class TestGetToken:
    """Test StackitAuthProvider.get_token."""

    def test_returns_cached_token_when_still_valid(self):
        provider = StackitAuthProvider(AuthConfig())
        provider._token = "cached-token"
        provider._expires_at = time.time() + 1000

        with patch.object(provider, "_get_token_from_stackit") as mock_fetch:
            token = provider.get_token()

        assert token == "cached-token"
        mock_fetch.assert_not_called()

    def test_fetches_new_token_when_expired(self):
        provider = StackitAuthProvider(AuthConfig())
        provider._token = "old-token"
        provider._expires_at = time.time() - 10

        with patch.object(provider, "_get_token_from_stackit", return_value=("new-token", 3600)):
            token = provider.get_token()

        assert token == "new-token"
        assert provider._token == "new-token"
        assert len(provider._tokens) == 1
        assert provider._tokens[0]["token"] == "new-token"

    def test_get_auth_header_uses_get_token(self):
        provider = StackitAuthProvider(AuthConfig())

        with patch.object(provider, "_get_token_from_stackit", return_value=("header-token", 3600)):
            header = provider.get_auth_header()

        assert header == {"Authorization": "Bearer header-token"}
