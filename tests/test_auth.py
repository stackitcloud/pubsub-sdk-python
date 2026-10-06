"""Tests for the auth module."""

from __future__ import annotations

import json
import sys
import time
from unittest.mock import MagicMock, patch

import jwt
import pytest

from pubsub_sdk_python.auth import AuthConfig, StackitAuthProvider
from pubsub_sdk_python.errors import PubSubAuthenticationError

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


def _make_jwt(lifetime: float = 900) -> str:
    """Build an unsigned-verifiable JWT that expires `lifetime` seconds from now."""
    return jwt.encode({"exp": int(time.time() + lifetime)}, "secret", algorithm="HS256")


def _mock_key_auth_cls(access_token: str) -> MagicMock:
    """Build a mock standing in for stackit.core.auth_methods.key_auth.KeyAuth."""
    mock_instance = MagicMock()
    mock_instance.access_token = access_token
    return MagicMock(return_value=mock_instance)


class TestGetTokenFromStackit:
    """Test StackitAuthProvider._get_token_from_stackit."""

    def test_success_with_inline_json(self):
        config = AuthConfig(service_account_key=json.dumps(SAMPLE_SA_KEY))
        provider = StackitAuthProvider(config)

        access_token = _make_jwt(900)
        with patch(
            "stackit.core.auth_methods.key_auth.KeyAuth",
            _mock_key_auth_cls(access_token),
        ):
            token, expires_in = provider._get_token_from_stackit()

        assert token == access_token
        assert 895 < expires_in <= 900

    def test_success_with_key_file_path(self, tmp_path):
        key_path = tmp_path / "sa-key.json"
        key_path.write_text(json.dumps(SAMPLE_SA_KEY))
        config = AuthConfig(service_account_key=str(key_path))
        provider = StackitAuthProvider(config)

        access_token = _make_jwt(900)
        with patch(
            "stackit.core.auth_methods.key_auth.KeyAuth",
            _mock_key_auth_cls(access_token),
        ):
            token, expires_in = provider._get_token_from_stackit()

        assert token == access_token
        assert 895 < expires_in <= 900

    def test_reuses_key_auth_and_forces_refresh(self):
        config = AuthConfig(service_account_key=json.dumps(SAMPLE_SA_KEY))
        provider = StackitAuthProvider(config)
        key_auth_cls = _mock_key_auth_cls(_make_jwt(900))
        refreshed_token = _make_jwt(1800)

        def _refresh() -> None:
            key_auth_cls.return_value.access_token = refreshed_token

        key_auth_cls.return_value._KeyAuth__refresh_token.side_effect = _refresh

        with patch("stackit.core.auth_methods.key_auth.KeyAuth", key_auth_cls):
            provider._get_token_from_stackit()
            token, expires_in = provider._get_token_from_stackit()

        key_auth_cls.assert_called_once()
        key_auth_cls.return_value._KeyAuth__refresh_token.assert_called_once()
        assert token == refreshed_token
        assert 1795 < expires_in <= 1800

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
