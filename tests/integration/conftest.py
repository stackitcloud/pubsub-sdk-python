"""Integration tests for the STACKIT PubSub Python SDK.

Requires the following secrets to be set:
- SA_KEY_B64: Base64 encoded service account key JSON
- PROJECT_ID: STACKIT project ID
- TOKEN_URL: OAuth token endpoint
- REGION: API region (e.g. eu01)
"""

from __future__ import annotations

import base64
import contextlib
import os
import time
from pathlib import Path

import httpx
import pytest

from pubsub_python_sdk.auth import AuthConfig
from pubsub_python_sdk.client import PubSubClient
from pubsub_python_sdk.retry import with_retry

SA_KEY_PATH = "sa-key.json"
SA_EMAIL = "break-test-jennifer-qzj3sci8@sa.stackit.cloud"


def _load_sa_key() -> str:
    """Load the SA_KEY from environment or file."""
    sa_key_b64 = os.environ.get("SA_KEY_B64")
    if sa_key_b64 and sa_key_b64.strip():
        return base64.b64decode(sa_key_b64.strip()).decode("utf-8")

    sa_key = os.environ.get("SA_KEY")
    if sa_key and sa_key.strip():
        return sa_key.strip()

    sa_key_path = os.environ.get("SA_KEY_PATH", SA_KEY_PATH)
    path = Path(sa_key_path)
    if path.exists():
        return path.read_text().strip()

    raise ValueError("SA_KEY_B64 or SA_KEY not found in environment or as file")


def _get_control_plane_url() -> str:
    """Get the control plane base URL from environment or construct from REGION."""
    url = os.environ.get("CONTROL_PLANE_URL")
    if url:
        return url
    region = os.environ.get("REGION", "eu01")
    return f"https://pubsub-dev.api.{region}.qa.stackit.cloud"


def _create_auth_header(auth_config: AuthConfig) -> dict[str, str]:
    """Create an auth header using the StackitAuthProvider."""
    from pubsub_python_sdk.auth import StackitAuthProvider

    provider = StackitAuthProvider(auth_config)
    return provider.get_auth_header()


@with_retry
def _create_topic(
    control_plane_url: str,
    project_id: str,
    region: str,
    auth_header: dict[str, str],
    display_name: str | None = None,
) -> str:
    """Create a topic via the control plane API and return its ID."""
    if display_name is None:
        display_name = f"sdk-test-{int(time.time())}"

    payload = {
        "displayName": display_name,
        "description": "Created by SDK integration tests",
    }

    url = f"{control_plane_url}/v1beta/projects/{project_id}/regions/{region}/topics"

    with httpx.Client() as client:
        response = client.post(
            url,
            json=payload,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()
        topic = response.json()
        return topic["id"]


@with_retry
def _create_subscription(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_header: dict[str, str],
    display_name: str | None = None,
) -> str:
    """Create a subscription via the control plane API and return its ID."""
    if display_name is None:
        display_name = f"sdk-sub-{int(time.time())}"

    payload = {
        "displayName": display_name,
        "description": "Created by SDK integration tests",
    }

    url = (
        f"{control_plane_url}/v1beta/projects/"
        f"{project_id}/regions/{region}/topics/"
        f"{topic_id}/subscriptions"
    )

    with httpx.Client() as client:
        response = client.post(
            url,
            json=payload,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()
        subscription = response.json()
        return subscription["id"]


@with_retry
def _get_resource(url: str, auth_header: dict[str, str]) -> dict:
    """Fetch a topic or subscription resource, retrying on transient network errors."""
    with httpx.Client() as client:
        response = client.get(
            url,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()


def _wait_for_resource_active(
    control_plane_url: str,
    project_id: str,
    region: str,
    resource_type: str,
    resource_id: str,
    auth_header: dict[str, str],
    poll_interval: float = 10.0,
    timeout: float = 600.0,
    topic_id: str | None = None,
) -> None:
    """Poll a topic or subscription status until it's active or timeout is reached."""
    start_time = time.time()

    if resource_type == "subscriptions" and topic_id:
        url = (
            f"{control_plane_url}/v1beta/projects/"
            f"{project_id}/regions/{region}/topics/"
            f"{topic_id}/subscriptions/{resource_id}"
        )
    else:
        url = (
            f"{control_plane_url}/v1beta/projects/"
            f"{project_id}/regions/{region}/"
            f"{resource_type}/{resource_id}"
        )

    while time.time() - start_time < timeout:
        resource = _get_resource(url, auth_header)
        state = resource.get("state")

        if state == "active":
            return
        if state == "deleting":
            raise RuntimeError(f"{resource_type} entered deleting state: {resource_id}")

        time.sleep(poll_interval)

    raise TimeoutError(f"{resource_type} {resource_id} did not become active within {timeout}s")


@with_retry
def _delete_topic(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_header: dict[str, str],
) -> None:
    """Delete a topic via the control plane API (cascades to subscriptions)."""
    url = f"{control_plane_url}/v1beta/projects/{project_id}/regions/{region}/topics/{topic_id}"

    with httpx.Client() as client:
        response = client.delete(
            url,
            headers={**auth_header, "Content-Type": "application/json"},
            params={"force": True},
            timeout=30.0,
        )
        if response.status_code not in (202, 404):
            response.raise_for_status()


@with_retry
def _list_subscriptions(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_header: dict[str, str],
) -> list[dict]:
    """List all subscriptions for a topic."""
    url = (
        f"{control_plane_url}/v1beta/projects/"
        f"{project_id}/regions/{region}/topics/"
        f"{topic_id}/subscriptions"
    )

    with httpx.Client() as client:
        response = client.get(
            url,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("subscriptions", [])


@with_retry
def _delete_subscription(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    subscription_id: str,
    auth_header: dict[str, str],
) -> None:
    """Delete a subscription via the control plane API."""
    url = (
        f"{control_plane_url}/v1beta/projects/"
        f"{project_id}/regions/{region}/topics/"
        f"{topic_id}/subscriptions/{subscription_id}"
    )

    with httpx.Client() as client:
        response = client.delete(
            url,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        if response.status_code not in (202, 404):
            response.raise_for_status()


@with_retry
def _grant_topic_publisher_access(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_header: dict[str, str],
    email: str,
) -> None:
    """Grant publisher access to a topic via the control plane API."""
    url = (
        f"{control_plane_url}/v1beta/projects/"
        f"{project_id}/regions/{region}/topics/"
        f"{topic_id}/publishers"
    )

    payload = {"emailAddress": email}

    with httpx.Client() as client:
        response = client.patch(
            url,
            json=payload,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()


@with_retry
def _grant_subscription_subscriber_access(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    subscription_id: str,
    auth_header: dict[str, str],
    email: str,
) -> None:
    """Grant subscriber access to a subscription via the control plane API."""
    url = (
        f"{control_plane_url}/v1beta/projects/"
        f"{project_id}/regions/{region}/topics/"
        f"{topic_id}/subscriptions/{subscription_id}/subscribers"
    )

    payload = {"emailAddress": email}

    with httpx.Client() as client:
        response = client.patch(
            url,
            json=payload,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()


@pytest.fixture(scope="session")
def sa_key() -> str:
    """Load the service account key."""
    return _load_sa_key()


@pytest.fixture(scope="session")
def auth_config(sa_key: str) -> AuthConfig:
    """Create AuthConfig from SA_KEY."""
    return AuthConfig(
        service_account_key=sa_key,
        token_endpoint=os.environ.get(
            "TOKEN_URL", "https://accounts.qa.stackit.cloud/oauth/v2/token"
        ),
    )


@pytest.fixture(scope="session")
def control_plane_url() -> str:
    """Get the control plane URL from environment or construct from REGION."""
    return _get_control_plane_url()


@pytest.fixture(scope="session")
def project_id() -> str:
    """Get the project ID from environment."""
    pid = os.environ.get("PROJECT_ID")
    if not pid:
        raise ValueError("PROJECT_ID environment variable is required")
    return pid


@pytest.fixture(scope="session")
def region() -> str:
    """Get the region from environment."""
    return os.environ.get("REGION", "eu01")


@pytest.fixture(scope="session")
def topic_id(
    control_plane_url: str,
    project_id: str,
    region: str,
    auth_config: AuthConfig,
):
    """Create a topic once for all tests in this run, then delete it."""
    auth_header = _create_auth_header(auth_config)

    new_topic_id = _create_topic(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        auth_header=auth_header,
        display_name="sdk-integration-test-shared",
    )
    print(f"Created topic: {new_topic_id}")

    _wait_for_resource_active(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        resource_type="topics",
        resource_id=new_topic_id,
        auth_header=auth_header,
    )
    print(f"Topic {new_topic_id} is active")

    _grant_topic_publisher_access(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        topic_id=new_topic_id,
        auth_header=auth_header,
        email=SA_EMAIL,
    )
    print(f"Granted publisher access for {SA_EMAIL} on topic {new_topic_id}")

    yield new_topic_id

    print(f"Deleting shared topic: {new_topic_id}")
    try:
        _delete_topic(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=new_topic_id,
            auth_header=auth_header,
        )
        print(f"Deleted shared topic: {new_topic_id}")
    except Exception as e:
        print(f"Failed to delete shared topic {new_topic_id}: {e}")


@pytest.fixture(scope="session")
def base_url(topic_id: str) -> str:
    """Get the base URL from environment or construct from REGION and TOPIC_ID."""
    base_url = os.environ.get("BASE_URL")
    if base_url:
        return base_url
    region = os.environ.get("REGION", "eu01")
    return f"https://{topic_id}.pubsub.{region}.dev.onstackit.cloud"


@pytest.fixture()
def auth_header(auth_config: AuthConfig) -> dict[str, str]:
    """Create an auth header for API calls."""
    return _create_auth_header(auth_config)


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


@pytest.fixture(scope="session")
def subscription_id(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_config: AuthConfig,
):
    """Create a subscription once and reuse for all tests that need it."""
    auth_header = _create_auth_header(auth_config)

    existing_subs = _list_subscriptions(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        topic_id=topic_id,
        auth_header=auth_header,
    )

    if existing_subs:
        sub_id = existing_subs[0]["id"]
        print(f"Reusing existing subscription: {sub_id}")

        try:
            _wait_for_resource_active(
                control_plane_url=control_plane_url,
                project_id=project_id,
                region=region,
                resource_type="subscriptions",
                resource_id=sub_id,
                auth_header=auth_header,
                poll_interval=10.0,
                timeout=60.0,
                topic_id=topic_id,
            )
            print(f"Subscription {sub_id} is active")
        except TimeoutError:
            print(f"Subscription {sub_id} not active, creating new subscription")
            with contextlib.suppress(Exception):
                _delete_subscription(
                    control_plane_url=control_plane_url,
                    project_id=project_id,
                    region=region,
                    topic_id=topic_id,
                    subscription_id=sub_id,
                    auth_header=auth_header,
                )
            existing_subs = []

    if not existing_subs:
        sub_id = _create_subscription(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=topic_id,
            auth_header=auth_header,
            display_name="sdk-sub-integration-shared",
        )
        print(f"Created subscription: {sub_id}")

        _wait_for_resource_active(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            resource_type="subscriptions",
            resource_id=sub_id,
            auth_header=auth_header,
            topic_id=topic_id,
        )
        print(f"Subscription {sub_id} is active")

        _grant_subscription_subscriber_access(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=topic_id,
            subscription_id=sub_id,
            auth_header=auth_header,
            email=SA_EMAIL,
        )
        print(f"Granted subscriber access for {SA_EMAIL} on subscription {sub_id}")

    else:
        sub_id = existing_subs[0]["id"]

    yield sub_id


@pytest.fixture()
async def subscriber(pubsub_client: PubSubClient, subscription_id: str):
    """Create a PubSubSubscriber instance for each test using the shared subscription."""
    sub = pubsub_client.get_subscriber(subscription_id=subscription_id)
    yield sub
    await sub.close()


@pytest.fixture(autouse=True)
async def purge_between_tests(pubsub_client: PubSubClient):
    """Purge topic messages between tests using the data plane API."""
    yield

    pub = pubsub_client.get_publisher()
    try:
        await pub.purge_topic()
        print(f"Purged topic: {pubsub_client.topic_id}")
    except Exception as e:
        print(f"Failed to purge topic: {e}")
    finally:
        await pub.close()
