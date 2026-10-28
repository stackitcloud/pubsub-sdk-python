# Copyright 2026 Schwarz Digits Cloud GmbH & Co. KG
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Integration tests for the STACKIT PubSub Python SDK.

Requires the following secrets to be set:
- SA_KEY_B64: Base64 encoded service account key JSON
- STACKIT_PROJECT_ID: STACKIT project ID
"""

from __future__ import annotations

import base64
import contextlib
import json
import os
import time
from pathlib import Path

import httpx
import pytest

from pubsub_sdk_python.auth import AuthConfig, StackitAuthProvider
from pubsub_sdk_python.client import PubSubClient
from pubsub_sdk_python.errors import PubSubServerError
from pubsub_sdk_python.retry import RetryConfig, with_retry

SA_KEY_PATH = "sa-key.json"

CONTROL_PLANE_RETRY_CONFIG = RetryConfig(
    max_retries=6,
    base_delay=2.0,
    max_delay=30.0,
    multiplier=2.0,
)


def _log(msg: str) -> None:
    """Print an integration test log message on its own line."""
    print(f"\n[integration] {msg}", flush=True)


def _format_error(exc: BaseException) -> str:
    """Format an exception with its HTTP response body and underlying cause."""
    details = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        body = exc.response.text.strip()
        if body:
            details += f" (response body: {body})"
    if exc.__cause__ is not None:
        details += f" | caused by {_format_error(exc.__cause__)}"
    return details


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
    provider = StackitAuthProvider(auth_config)
    return provider.get_auth_header()


def _raise_for_status(response: httpx.Response) -> None:
    """Raise HTTPStatusError with response body details if request failed."""
    if response.status_code in {429, 500, 502, 503, 504}:
        body = response.text.strip()
        raise PubSubServerError(f"Server error {response.status_code}: {body}")
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        body = response.text.strip()
        if body:
            raise httpx.HTTPStatusError(
                f"{exc} - response body: {body}",
                request=exc.request,
                response=exc.response,
            ) from None
        raise


@with_retry(config=CONTROL_PLANE_RETRY_CONFIG)
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
        _raise_for_status(response)
        topic = response.json()
        return topic["id"]


@with_retry(config=CONTROL_PLANE_RETRY_CONFIG)
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
        _raise_for_status(response)
        subscription = response.json()
        return subscription["id"]


def _get_resource(url: str, auth_header: dict[str, str]) -> dict:
    """Fetch a topic or subscription resource."""
    with httpx.Client() as client:
        response = client.get(
            url,
            headers={**auth_header, "Content-Type": "application/json"},
            timeout=30.0,
        )
        _raise_for_status(response)
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
        try:
            resource = _get_resource(url, auth_header)
        except (httpx.TransportError, PubSubServerError) as exc:
            # The resource may not be reachable yet right after creation
            # (e.g. backend routing not provisioned) - keep polling until timeout.
            _log(f"{resource_type} {resource_id} not reachable yet, retrying: {exc}")
            time.sleep(poll_interval)
            continue

        state = resource.get("state")

        if state == "active":
            return
        if state == "deleting":
            raise RuntimeError(f"{resource_type} entered deleting state: {resource_id}")

        _log(f"Waiting for {resource_type} {resource_id} to become active (state: {state})")
        time.sleep(poll_interval)

    raise TimeoutError(f"{resource_type} {resource_id} did not become active within {timeout}s")


@with_retry(config=CONTROL_PLANE_RETRY_CONFIG)
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
            _raise_for_status(response)


@with_retry(config=CONTROL_PLANE_RETRY_CONFIG)
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
        _raise_for_status(response)
        data = response.json()
        return data.get("subscriptions", [])


@with_retry(config=CONTROL_PLANE_RETRY_CONFIG)
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
            _raise_for_status(response)


@with_retry(config=CONTROL_PLANE_RETRY_CONFIG)
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
        _raise_for_status(response)


@with_retry(config=CONTROL_PLANE_RETRY_CONFIG)
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
        _raise_for_status(response)


@pytest.fixture(scope="session")
def sa_key() -> str:
    """Load the service account key."""
    return _load_sa_key()


@pytest.fixture(scope="session")
def sa_email(sa_key: str) -> str:
    """Extract the service account email (credentials.iss) from SA_KEY."""
    return json.loads(sa_key)["credentials"]["iss"]


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
def auth_provider(auth_config: AuthConfig) -> StackitAuthProvider:
    """Create a session-scoped StackitAuthProvider for control plane API calls."""
    return StackitAuthProvider(auth_config)


@pytest.fixture(scope="session")
def control_plane_url() -> str:
    """Get the control plane URL from environment or construct from REGION."""
    return _get_control_plane_url()


@pytest.fixture(scope="session")
def project_id() -> str:
    """Get the project ID from environment."""
    pid = os.environ.get("STACKIT_PROJECT_ID")
    if not pid:
        raise ValueError("STACKIT_PROJECT_ID environment variable is required")
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
    auth_provider: StackitAuthProvider,
    sa_email: str,
):
    """Create a topic once for all tests in this run, then delete it."""
    new_topic_id = _create_topic(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        auth_header=auth_provider.get_auth_header(),
        display_name="sdk-integration-test-shared",
    )
    _log(f"Created topic: {new_topic_id}")

    _wait_for_resource_active(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        resource_type="topics",
        resource_id=new_topic_id,
        auth_header=auth_provider.get_auth_header(),
    )
    _log(f"Topic {new_topic_id} is active")

    _grant_topic_publisher_access(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        topic_id=new_topic_id,
        auth_header=auth_provider.get_auth_header(),
        email=sa_email,
    )
    _log(f"Granted publisher access for {sa_email} on topic {new_topic_id}")

    yield new_topic_id

    _log(f"Deleting shared topic: {new_topic_id}")
    try:
        subs = _list_subscriptions(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=new_topic_id,
            auth_header=auth_provider.get_auth_header(),
        )
        for sub in subs:
            sub_id = sub.get("id")
            if sub_id:
                try:
                    _delete_subscription(
                        control_plane_url=control_plane_url,
                        project_id=project_id,
                        region=region,
                        topic_id=new_topic_id,
                        subscription_id=sub_id,
                        auth_header=auth_provider.get_auth_header(),
                    )
                    _log(f"Deleted subscription {sub_id} on topic {new_topic_id}")
                except Exception as sub_exc:
                    _log(
                        f"Failed to delete subscription {sub_id} on topic {new_topic_id}: "
                        f"{_format_error(sub_exc)}"
                    )
    except Exception as list_exc:
        _log(
            f"Failed to list subscriptions before deleting topic {new_topic_id}: "
            f"{_format_error(list_exc)}"
        )

    try:
        _delete_topic(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=new_topic_id,
            auth_header=auth_provider.get_auth_header(),
        )
        _log(f"Deleted shared topic: {new_topic_id}")
    except Exception as e:
        _log(f"Failed to delete shared topic {new_topic_id}: {_format_error(e)}")


@pytest.fixture(scope="session")
def base_url(topic_id: str) -> str:
    """Get the base URL from environment or construct from REGION and TOPIC_ID."""
    base_url = os.environ.get("BASE_URL")
    if base_url:
        return base_url
    region = os.environ.get("REGION", "eu01")
    return f"https://{topic_id}.pubsub.{region}.dev.onstackit.cloud"


@pytest.fixture()
def auth_header(auth_provider: StackitAuthProvider) -> dict[str, str]:
    """Create an auth header for API calls."""
    return auth_provider.get_auth_header()


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
    auth_provider: StackitAuthProvider,
    sa_email: str,
):
    """Create a subscription once and reuse for all tests that need it."""
    existing_subs = _list_subscriptions(
        control_plane_url=control_plane_url,
        project_id=project_id,
        region=region,
        topic_id=topic_id,
        auth_header=auth_provider.get_auth_header(),
    )

    if existing_subs:
        sub_id = existing_subs[0]["id"]
        _log(f"Reusing existing subscription: {sub_id}")

        try:
            _wait_for_resource_active(
                control_plane_url=control_plane_url,
                project_id=project_id,
                region=region,
                resource_type="subscriptions",
                resource_id=sub_id,
                auth_header=auth_provider.get_auth_header(),
                poll_interval=10.0,
                timeout=60.0,
                topic_id=topic_id,
            )
            _log(f"Subscription {sub_id} is active")
        except TimeoutError:
            _log(f"Subscription {sub_id} not active, creating new subscription")
            with contextlib.suppress(Exception):
                _delete_subscription(
                    control_plane_url=control_plane_url,
                    project_id=project_id,
                    region=region,
                    topic_id=topic_id,
                    subscription_id=sub_id,
                    auth_header=auth_provider.get_auth_header(),
                )
            existing_subs = []

    if not existing_subs:
        sub_id = _create_subscription(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=topic_id,
            auth_header=auth_provider.get_auth_header(),
            display_name="sdk-sub-integration-shared",
        )
        _log(f"Created subscription: {sub_id}")

        _wait_for_resource_active(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            resource_type="subscriptions",
            resource_id=sub_id,
            auth_header=auth_provider.get_auth_header(),
            topic_id=topic_id,
        )
        _log(f"Subscription {sub_id} is active")

        _grant_subscription_subscriber_access(
            control_plane_url=control_plane_url,
            project_id=project_id,
            region=region,
            topic_id=topic_id,
            subscription_id=sub_id,
            auth_header=auth_provider.get_auth_header(),
            email=sa_email,
        )
        _log(f"Granted subscriber access for {sa_email} on subscription {sub_id}")

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
        _log(f"Purged topic: {pubsub_client.topic_id}")
    except Exception as e:
        _log(f"Failed to purge topic: {_format_error(e)}")
    finally:
        await pub.close()
