"""Integration tests for the STACKIT PubSub Python SDK.

Requires the following secrets to be set:
- SA_KEY: Service account key JSON
- PROJECT_ID: STACKIT project ID
- TOKEN_URL: OAuth token endpoint
- REGION: API region (e.g. eu01)
- BASE_URL: API base URL
- TOPIC_ID: Topic ID for testing
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pubsub_python_sdk.auth import AuthConfig
from pubsub_python_sdk.client import PubSubClient


def _load_sa_key() -> str:
    """Load the SA_KEY from environment or file."""
    sa_key = os.environ.get("SA_KEY")
    if sa_key:
        return sa_key
    sa_key_path = os.environ.get("SA_KEY_PATH", "sa-key.json")
    path = Path(sa_key_path)
    if path.exists():
        return path.read_text()
    raise ValueError("SA_KEY not found in environment or as file")


@pytest.fixture()
def sa_key() -> str:
    """Load the service account key."""
    return _load_sa_key()


@pytest.fixture()
def auth_config(sa_key: str) -> AuthConfig:
    """Create AuthConfig from SA_KEY."""
    return AuthConfig(
        service_account_key=sa_key,
        token_endpoint=os.environ.get(
            "TOKEN_URL", "https://accounts.qa.stackit.cloud/oauth/v2/token"
        ),
    )


@pytest.fixture()
def base_url() -> str:
    """Get the base URL from environment or construct from REGION."""
    base_url = os.environ.get("BASE_URL")
    if base_url:
        return base_url
    region = os.environ.get("REGION", "eu01")
    return f"https://pubsub-dev.api.{region}.qa.stackit.cloud"


@pytest.fixture()
def topic_id() -> str:
    """Get the topic ID from environment."""
    topic_id = os.environ.get("TOPIC_ID")
    if topic_id is None:
        raise ValueError("TOPIC_ID environment variable is required")
    return topic_id


@pytest.fixture()
def pubsub_client(base_url: str, topic_id: str, auth_config: AuthConfig) -> PubSubClient:
    """Create a PubSubClient for integration tests."""
    return PubSubClient(
        topic_id=topic_id,
        auth_config=auth_config,
        base_url=base_url,
    )


@pytest.fixture()
async def publisher(pubsub_client: PubSubClient):
    """Create and return a publisher instance."""
    pub = pubsub_client.get_publisher()
    yield pub
    await pub.close()
