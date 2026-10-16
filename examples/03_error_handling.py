#!/usr/bin/env python3
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

"""Demonstrate robust error handling with the STACKIT PubSub SDK's exceptions.

All SDK exceptions subclass ``PubSubError``. This example is split into two
parts:

Part 1 - local validation errors (always runs, no network or credentials
required). The SDK validates input *before* making any HTTP request, so
these exceptions can be triggered and caught without a live topic,
subscription, or service account key.

Part 2 - live error handling against the real dataplane API (only runs if
STACKIT_SERVICE_ACCOUNT_KEY_PATH and STACKIT_TOPIC_ID are set). It pulls
from a deliberately non-existent subscription with a short retry budget to
show how PubSubNotFoundError, PubSubAuthenticationError, and
PubSubRetryError (retry exhaustion) are distinguished and handled in
production code.

Prerequisites
-------------
Part 1 has no prerequisites.

Part 2 optionally uses:
    STACKIT_SERVICE_ACCOUNT_KEY_PATH   Path to a STACKIT service account key
                                        JSON file.
    STACKIT_TOPIC_ID                   ID of an existing PubSub topic.

Optional environment variables:
    STACKIT_TOKEN_ENDPOINT   OAuth2 token endpoint override.
    STACKIT_BASE_URL         Dataplane base URL override. Defaults to
                              "https://{topic_id}.pubsub.eu01.onstackit.cloud".

Run
---
    python examples/03_error_handling.py
"""

from __future__ import annotations

import asyncio
import os

from pubsub_sdk_python import (
    AuthConfig,
    PublishMessage,
    PubSubAuthenticationError,
    PubSubError,
    PubSubNotFoundError,
    PubSubPublisher,
    PubSubRetryError,
    PubSubSubscriber,
    PubSubValidationError,
    RetryConfig,
    StackitAuthProvider,
)

DEFAULT_TOKEN_ENDPOINT = "https://authorization.stackit.cloud/auth/token"
DEFAULT_BASE_URL = "https://{topicId}.pubsub.eu01.onstackit.cloud"
PLACEHOLDER_TOPIC_ID = "example-topic"


async def demonstrate_local_validation_errors() -> None:
    """Trigger and catch PubSubValidationError without any network access.

    The SDK validates publish/pull/ack/nack arguments before issuing an
    HTTP request, so these calls fail fast and predictably even with a
    throwaway (unauthenticated) client.
    """
    print("Part 1: local validation errors (no network required)")

    # An unauthenticated provider is fine here: none of the calls below
    # reach the point of building an Authorization header.
    auth_provider = StackitAuthProvider(AuthConfig())
    publisher = PubSubPublisher(
        topic_id=PLACEHOLDER_TOPIC_ID,
        base_url=DEFAULT_BASE_URL.format(topicId=PLACEHOLDER_TOPIC_ID),
        auth_provider=auth_provider,
    )
    subscriber = PubSubSubscriber(
        subscription_id="example-subscription",
        base_url=DEFAULT_BASE_URL.format(topicId=PLACEHOLDER_TOPIC_ID),
        auth_provider=auth_provider,
    )

    try:
        await publisher.publish([])
    except PubSubValidationError as exc:
        print(f"  caught expected error for an empty publish batch: {exc}")

    try:
        await publisher.publish([PublishMessage.from_plain("x")] * 101)
    except PubSubValidationError as exc:
        print(f"  caught expected error for an oversized publish batch: {exc}")

    try:
        await subscriber.pull(max_messages=0)
    except PubSubValidationError as exc:
        print(f"  caught expected error for an out-of-range max_messages: {exc}")

    try:
        await subscriber.ack([])
    except PubSubValidationError as exc:
        print(f"  caught expected error for an empty ack list: {exc}")

    await publisher.close()
    await subscriber.close()


async def demonstrate_live_error_handling() -> None:
    """Show how live API errors are distinguished, using a short retry budget.

    Skips itself with an explanatory message if the required environment
    variables are not set, rather than failing with a network traceback.
    """
    print("Part 2: live error handling against the dataplane API")

    key_path = os.environ.get("STACKIT_SERVICE_ACCOUNT_KEY_PATH")
    topic_id = os.environ.get("STACKIT_TOPIC_ID")
    if not key_path or not topic_id:
        print(
            "  skipped: set STACKIT_SERVICE_ACCOUNT_KEY_PATH and STACKIT_TOPIC_ID "
            "to exercise this part against a real topic."
        )
        return

    try:
        with open(key_path, encoding="utf-8") as key_file:
            service_account_key = key_file.read()
    except OSError as exc:
        print(f"  skipped: could not read service account key at {key_path}: {exc}")
        return

    token_endpoint = os.environ.get("STACKIT_TOKEN_ENDPOINT", DEFAULT_TOKEN_ENDPOINT)
    auth_config = AuthConfig(service_account_key=service_account_key, token_endpoint=token_endpoint)
    auth_provider = StackitAuthProvider(auth_config)
    base_url = os.environ.get("STACKIT_BASE_URL", DEFAULT_BASE_URL).format(topicId=topic_id)

    # A small retry budget keeps this demonstration fast; production code
    # would typically use the default RetryConfig() or tune it deliberately.
    retry_config = RetryConfig(max_retries=1, base_delay=0.2, max_delay=1.0)

    # Deliberately non-existent subscription id to provoke a 404.
    subscriber = PubSubSubscriber(
        subscription_id="does-not-exist-example-subscription",
        base_url=base_url,
        auth_provider=auth_provider,
        retry_config=retry_config,
    )

    try:
        await subscriber.pull(max_messages=1)
    except PubSubAuthenticationError as exc:
        # HTTP 401/403, or the service account key could not be exchanged
        # for a token.
        print(f"  caught authentication error - check the service account key: {exc}")
    except PubSubNotFoundError as exc:
        # HTTP 404 - expected here, since the subscription id is bogus.
        print(f"  caught not-found error as expected for a bogus subscription id: {exc}")
    except PubSubRetryError as exc:
        # All retries for transient network/server errors were exhausted;
        # exc.causes holds the underlying exceptions for diagnostics.
        print(f"  caught retry-exhaustion error: {exc} (causes={exc.causes})")
    except PubSubError as exc:
        # Catch-all for any other SDK error not handled above.
        print(f"  caught unexpected PubSub SDK error: {exc}")
    finally:
        await subscriber.close()


async def main() -> None:
    """Run both parts of the error-handling demonstration."""
    await demonstrate_local_validation_errors()
    print()
    await demonstrate_live_error_handling()


if __name__ == "__main__":
    asyncio.run(main())
