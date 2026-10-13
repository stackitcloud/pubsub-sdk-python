#!/usr/bin/env python3
"""Publish messages to a STACKIT PubSub topic.

This example shows the standard SDK workflow:

1. Build an ``AuthConfig`` from a STACKIT service account key file whose
   path comes from an environment variable (never hardcode key material).
2. Instantiate ``PubSubClient`` and derive a ``PubSubPublisher`` from it.
3. Publish a small batch of messages and handle the SDK's specific
   exception types individually.

Prerequisites
-------------
Environment variables:
    STACKIT_SERVICE_ACCOUNT_KEY_PATH   Path to a STACKIT service account key
                                        JSON file.
    STACKIT_TOPIC_ID                   ID of an existing PubSub topic that
                                        the service account may publish to.

Optional environment variables:
    STACKIT_TOKEN_ENDPOINT   OAuth2 token endpoint override (defaults to the
                              SDK's built-in STACKIT authorization endpoint).
    STACKIT_BASE_URL         Dataplane base URL override. Defaults to
                              "https://{topic_id}.pubsub.eu01.onstackit.cloud".

Obtaining a service account key
--------------------------------
Create or rotate a service account key using the STACKIT Portal or the
STACKIT CLI (`stackit service-account key create ...`), then point
STACKIT_SERVICE_ACCOUNT_KEY_PATH at the downloaded JSON file. The key file
itself is never read, printed, or embedded by this script's source code -
only its filesystem path is taken from the environment.

Run
---
    python examples/01_publish_message.py
"""

from __future__ import annotations

import asyncio
import os
import sys

from pubsub_sdk_python import (
    AuthConfig,
    PublishMessage,
    PubSubAuthenticationError,
    PubSubClient,
    PubSubError,
    PubSubRetryError,
    PubSubValidationError,
)

DEFAULT_TOKEN_ENDPOINT = "https://authorization.stackit.cloud/auth/token"
DEFAULT_BASE_URL = "https://{topicId}.pubsub.eu01.onstackit.cloud"


def _require_env(name: str) -> str:
    """Return a required environment variable or exit with a clear message.

    Args:
        name: Name of the environment variable to look up.

    Returns:
        The environment variable's value.
    """
    value = os.environ.get(name)
    if not value:
        print(
            f"ERROR: required environment variable {name} is not set.\n"
            "See the module docstring in this file for setup instructions.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


def _build_auth_config() -> AuthConfig:
    """Build an AuthConfig by reading the service account key from disk.

    The key file path is sourced from STACKIT_SERVICE_ACCOUNT_KEY_PATH
    rather than being hardcoded, keeping credential material out of source
    control.

    Returns:
        A populated AuthConfig ready to use with PubSubClient.
    """
    key_path = _require_env("STACKIT_SERVICE_ACCOUNT_KEY_PATH")

    try:
        with open(key_path, encoding="utf-8") as key_file:
            service_account_key = key_file.read()
    except OSError as exc:
        print(f"ERROR: could not read service account key at {key_path}: {exc}", file=sys.stderr)
        sys.exit(1)

    token_endpoint = os.environ.get("STACKIT_TOKEN_ENDPOINT", DEFAULT_TOKEN_ENDPOINT)
    return AuthConfig(service_account_key=service_account_key, token_endpoint=token_endpoint)


async def main() -> None:
    """Publish two example messages to the configured topic."""
    topic_id = _require_env("STACKIT_TOPIC_ID")
    auth_config = _build_auth_config()
    base_url = os.environ.get("STACKIT_BASE_URL", DEFAULT_BASE_URL)

    client = PubSubClient(topic_id=topic_id, auth_config=auth_config, base_url=base_url)

    messages = [
        PublishMessage.from_plain("hello from the STACKIT PubSub Python SDK"),
        PublishMessage.from_plain("a second example message"),
    ]

    try:
        async with client.get_publisher() as publisher:
            await publisher.publish(messages)
            print(f"Published {len(messages)} message(s) successfully.")
    except PubSubValidationError as exc:
        # Bad request (HTTP 400/413) or local validation, e.g. an empty or
        # oversized message batch.
        print(f"Publish request was rejected as invalid: {exc}", file=sys.stderr)
        sys.exit(1)
    except PubSubAuthenticationError as exc:
        # HTTP 401/403, or the service account key could not be exchanged
        # for a token.
        print(f"Authentication failed - check the service account key: {exc}", file=sys.stderr)
        sys.exit(1)
    except PubSubRetryError as exc:
        # All automatic retries for transient network/server errors were
        # exhausted; exc.causes holds the underlying exceptions.
        print(
            f"Publish failed after exhausting retries: {exc} (causes={exc.causes})", file=sys.stderr
        )
        sys.exit(1)
    except PubSubError as exc:
        # Catch-all for any other SDK error not handled above.
        print(f"Unexpected PubSub SDK error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
