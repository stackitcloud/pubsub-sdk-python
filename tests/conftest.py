"""Pytest configuration and shared fixtures for PubSub SDK tests."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pubsub_sdk_python.auth import AuthConfig, StackitAuthProvider
from pubsub_sdk_python.models import PublishMessage


@pytest.fixture
def auth_config():
    """Create a test AuthConfig."""
    return AuthConfig(
        client_id="test-client-id",
        client_secret="test-client-secret",
        scope="dataplane",
    )


@pytest.fixture
def auth_provider(auth_config):
    """Create a StackitAuthProvider with mocked token."""
    provider = StackitAuthProvider(auth_config)
    provider._token = "Bearer test-token"
    provider._expires_at = 9999999999
    return provider


@pytest.fixture
def base_url():
    """Test base URL."""
    return "https://test-topic.pubsub.eu01.onstackit.cloud"


@pytest.fixture
def mock_http_client():
    """Create a mock async HTTP client."""
    client = AsyncMock(spec=httpx.AsyncClient)
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = {"messageIds": [1, 2, 3]}
    response.text = "OK"
    client.request = AsyncMock(return_value=response)
    return client


@pytest.fixture
def mock_response():
    """Create a mock HTTP response."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = {"messageIds": [1, 2, 3]}
    response.text = "OK"
    return response


@pytest.fixture
def sample_messages():
    """Create sample publish messages."""
    return [
        PublishMessage.from_plain("Hello, World!"),
        PublishMessage.from_plain("Second message"),
    ]


@pytest.fixture
def sample_ack_ids():
    """Create sample ack IDs."""
    return ["ack-1", "ack-2", "ack-3"]


@pytest.fixture
def sample_nack_ids():
    """Create sample nack IDs."""
    return ["nack-1", "nack-2"]


@pytest.fixture
def sample_pull_messages():
    """Create sample messages for pull responses."""
    return {
        "messages": [
            {
                "id": 1,
                "ackId": "ack-1",
                "data": "SGVsbG8sIFdvcmxkIQ==",
                "createTime": "2024-01-01T00:00:00Z",
                "deliveryAttempts": 1,
            },
            {
                "id": 2,
                "ackId": "ack-2",
                "data": "U2Vjb25kIG1lc3NhZ2U=",
                "createTime": "2024-01-01T00:01:00Z",
                "deliveryAttempts": 1,
            },
        ],
    }
