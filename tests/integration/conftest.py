"""Integration tests for the STACKIT PubSub Python SDK.

Requires the following secrets to be set:
- SA_KEY_B64: Base64 encoded service account key JSON
- PROJECT_ID: STACKIT project ID
- TOKEN_URL: OAuth token endpoint
- REGION: API region (e.g. eu01)
"""

from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path

import httpx
import pytest

from pubsub_python_sdk.auth import AuthConfig
from pubsub_python_sdk.client import PubSubClient

SA_KEY_PATH = "sa-key.json"
TOPIC_LIST_PATH = "integration-topic-list.json"
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


def _load_topic_list() -> list[str]:
    """Load existing topic IDs from the topic list file."""
    path = Path(TOPIC_LIST_PATH)
    if path.exists():
        try:
            data = json.loads(path.read_text())
            return data.get("topicIds", [])
        except (json.JSONDecodeError, KeyError):
            return []
    return []


def _save_topic_list(topic_ids: list[str]) -> None:
    """Save topic IDs to the topic list file."""
    path = Path(TOPIC_LIST_PATH)
    path.write_text(json.dumps({"topicIds": topic_ids}, indent=2))


def _create_auth_header(auth_config: AuthConfig) -> dict[str, str]:
    """Create an auth header using the StackitAuthProvider."""
    from pubsub_python_sdk.auth import StackitAuthProvider

    provider = StackitAuthProvider(auth_config)
    return provider.get_auth_header()


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
        with httpx.Client() as client:
            response = client.get(
                url,
                headers={**auth_header, "Content-Type": "application/json"},
                timeout=30.0,
            )
            response.raise_for_status()
            resource = response.json()
            state = resource.get("state")

            if state == "active":
                return
            if state == "deleting":
                raise RuntimeError(f"{resource_type} entered deleting state: {resource_id}")

        time.sleep(poll_interval)

    raise TimeoutError(f"{resource_type} {resource_id} did not become active within {timeout}s")


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
        # 202 is expected, 404 means already deleted
        if response.status_code not in (202, 404):
            response.raise_for_status()


def _list_subscriptions(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_header: dict[str, str],
) -> list[dict]:
    """List all subscriptions for a topic."""
    url = f"{control_plane_url}/v1beta/projects/{project_id}/regions/{region}/topics/{topic_id}/subscriptions"

    with httpx.Client() as client:
        response = client.get(url, headers={**auth_header, "Content-Type": "application/json"}, timeout=30.0)
        response.raise_for_status()
        data = response.json()
        return data.get("subscriptions", [])


def _delete_subscription(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    subscription_id: str,
    auth_header: dict[str, str],
) -> None:
    """Delete a subscription via the control plane API."""
    url = f"{control_plane_url}/v1beta/projects/{project_id}/regions/{region}/topics/{topic_id}/subscriptions/{subscription_id}"

    with httpx.Client() as client:
        response = client.delete(
            url,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        # 202 is expected, 404 means already deleted
        if response.status_code not in (202, 404):
            response.raise_for_status()


def _grant_topic_publisher_access(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_header: dict[str, str],
    email: str,
) -> None:
    """Grant publisher access to a topic via the control plane API."""
    url = f"{control_plane_url}/v1beta/projects/{project_id}/regions/{region}/topics/{topic_id}/publishers"

    payload = {"emailAddress": email}

    with httpx.Client() as client:
        response = client.patch(
            url,
            json=payload,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()


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
    url = f"{control_plane_url}/v1beta/projects/{project_id}/regions/{region}/topics/{topic_id}/subscriptions/{subscription_id}/subscribers"

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
    """Create a topic once, wait for it to be active, and reuse for all tests."""
    auth_header = _create_auth_header(auth_config)

    # Try to load existing topic ID
    existing_ids = _load_topic_list()
    if existing_ids:
        new_topic_id = existing_ids[0]
        print(f"Reusing existing topic: {new_topic_id}")

        # Verify topic is active
        try:
            _wait_for_resource_active(
                control_plane_url=control_plane_url,
                project_id=project_id,
                region=region,
                resource_type="topics",
                resource_id=new_topic_id,
                auth_header=auth_header,
                poll_interval=10.0,
                timeout=60.0,
            )
            print(f"Topic {new_topic_id} is active")
        except TimeoutError:
            # Topic is not active, delete and create new
            print(f"Topic {new_topic_id} not active, creating new topic")
            try:
                _delete_topic(
                    control_plane_url=control_plane_url,
                    project_id=project_id,
                    region=region,
                    topic_id=new_topic_id,
                    auth_header=auth_header,
                )
            except Exception:
                pass
            existing_ids = []

    # Create new topic if none exists
    if not existing_ids:
        new_topic_id = _create_topic(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            auth_header=auth_header,
            display_name="sdk-integration-test-shared",
        )
        print(f"Created topic: {new_topic_id}")

        # Wait for topic to be active
        _wait_for_resource_active(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            resource_type="topics",
            resource_id=new_topic_id,
            auth_header=auth_header,
        )
        print(f"Topic {new_topic_id} is active")

        # Grant publisher access to the service account
        _grant_topic_publisher_access(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=new_topic_id,
            auth_header=auth_header,
            email=SA_EMAIL,
        )
        print(f"Granted publisher access for {SA_EMAIL} on topic {new_topic_id}")

        # Save topic ID for reuse
        _save_topic_list([new_topic_id])

    else:
        new_topic_id = existing_ids[0]

    yield new_topic_id

    # Clean up: delete topic (cascades to subscriptions)
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
def subscriber(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_config: AuthConfig,
):
    """Create a subscription once and reuse for all tests that need it."""
    auth_header = _create_auth_header(auth_config)

    # Check if subscription already exists
    existing_subs = _list_subscriptions(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        topic_id=topic_id,
        auth_header=auth_header,
    )

    if existing_subs:
        sub_id = existing_subs[0]["name"].split("/")[-1]
        print(f"Reusing existing subscription: {sub_id}")

        # Verify subscription is active
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
            # Subscription not active, delete and create new
            print(f"Subscription {sub_id} not active, creating new subscription")
            try:
                _delete_subscription(
                    control_plane_url=control_plane_url,
                    project_id=project_id,
                    region=region,
                    topic_id=topic_id,
                    subscription_id=sub_id,
                    auth_header=auth_header,
                )
            except Exception:
                pass
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

        # Wait for subscription to be active
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

        # Grant subscriber access
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
        sub_id = existing_subs[0]["name"].split("/")[-1]

    yield sub_id

    # Note: subscription is deleted when topic is deleted (cascade)


@pytest.fixture(autouse=True)
def cleanup_between_tests(
    control_plane_url: str,
    project_id: str,
    region: str,
    topic_id: str,
    auth_config: AuthConfig,
):
    """Delete any leftover subscriptions between tests."""
    yield

    # Delete any subscriptions created during the test (they're deleted with the topic at the end)
    try:
        existing_subs = _list_subscriptions(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=topic_id,
            auth_header=_create_auth_header(auth_config),
        )
        for sub in existing_subs:
            sub_id = sub["name"].split("/")[-1]
            _delete_subscription(
                control_plane_url=control_plane_url,
                project_id=project_id,
                region=region,
                topic_id=topic_id,
                subscription_id=sub_id,
                auth_header=_create_auth_header(auth_config),
            )
            print(f"Deleted leftover subscription: {sub_id}")
    except Exception as e:
        print(f"Failed to cleanup subscriptions: {e}")
