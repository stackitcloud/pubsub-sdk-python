#!/usr/bin/env python3
"""Pull messages from a STACKIT PubSub subscription, then ack or nack them.

This example shows the standard SDK workflow for consuming messages:

1. Build an ``AuthConfig`` from a STACKIT service account key file whose
   path comes from an environment variable.
2. Instantiate ``PubSubClient`` and derive a ``PubSubSubscriber`` from it.
3. Pull a batch of messages, print their decoded payloads, acknowledge the
   ones that were processed successfully, and negatively acknowledge (nack)
   any that failed processing so they become eligible for redelivery.

Prerequisites
-------------
Environment variables:
    STACKIT_SERVICE_ACCOUNT_KEY_PATH   Path to a STACKIT service account key
                                        JSON file.
    STACKIT_TOPIC_ID                   ID of the PubSub topic that owns the
                                        subscription below.
    STACKIT_SUBSCRIPTION_ID            ID of an existing subscription that
                                        the service account may pull from.

Optional environment variables:
    STACKIT_TOKEN_ENDPOINT   OAuth2 token endpoint override (defaults to the
                              SDK's built-in STACKIT authorization endpoint).
    STACKIT_BASE_URL         Dataplane base URL override. Defaults to
                              "https://{topic_id}.pubsub.eu01.onstackit.cloud".

Obtaining a service account key
--------------------------------
Create or rotate a service account key using the STACKIT Portal or the
STACKIT CLI (`stackit service-account key create ...`), then point
STACKIT_SERVICE_ACCOUNT_KEY_PATH at the downloaded JSON file.

Run
---
    python examples/02_pull_and_ack.py
"""

from __future__ import annotations

import asyncio
import os
import sys

from pubsub_sdk_python import (
    AuthConfig,
    Message,
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


def _process_message(message: Message) -> bool:
    """Simulate application-specific processing of a single message.

    Replace this with real business logic. Returning False simulates a
    processing failure so the nack path below can be exercised.

    Args:
        message: The pulled message to process.

    Returns:
        True if processing succeeded and the message should be acked,
        False if it should be nacked for redelivery.
    """
    print(f"  processing message ack_id={message.ack_id}: {message.text!r}")
    return True


async def main() -> None:
    """Pull one batch of messages and ack/nack them based on processing outcome."""
    topic_id = _require_env("STACKIT_TOPIC_ID")
    subscription_id = _require_env("STACKIT_SUBSCRIPTION_ID")
    auth_config = _build_auth_config()
    base_url = os.environ.get("STACKIT_BASE_URL", DEFAULT_BASE_URL)

    client = PubSubClient(topic_id=topic_id, auth_config=auth_config, base_url=base_url)

    try:
        async with client.get_subscriber(subscription_id=subscription_id) as subscriber:
            messages = await subscriber.pull(max_messages=10)

            if not messages:
                print("No messages available to pull right now.")
                return

            print(f"Pulled {len(messages)} message(s).")

            acked: list[Message] = []
            nacked: list[Message] = []
            for message in messages:
                if _process_message(message):
                    acked.append(message)
                else:
                    nacked.append(message)

            if acked:
                await subscriber.ack_messages(acked)
                print(f"Acked {len(acked)} message(s).")
            if nacked:
                await subscriber.nack_messages(nacked)
                print(f"Nacked {len(nacked)} message(s) for redelivery.")
    except PubSubValidationError as exc:
        # Local/HTTP validation error, e.g. max_messages out of range or an
        # empty ack/nack list.
        print(f"Pull/ack request was rejected as invalid: {exc}", file=sys.stderr)
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
            f"Operation failed after exhausting retries: {exc} (causes={exc.causes})",
            file=sys.stderr,
        )
        sys.exit(1)
    except PubSubError as exc:
        # Catch-all for any other SDK error not handled above.
        print(f"Unexpected PubSub SDK error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
