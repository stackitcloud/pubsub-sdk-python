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

"""Main PubSub client combining Publisher and Subscriber functionality."""

from __future__ import annotations

from typing import Any

from pubsub_sdk_python.auth import AuthConfig, StackitAuthProvider
from pubsub_sdk_python.publisher import PubSubPublisher
from pubsub_sdk_python.subscriber import PubSubSubscriber


class PubSubClient:
    """Unified client for PUB/SUB operations against a STACKIT topic.

    Provides a single entry point for both publishing and subscribing,
    sharing authentication and HTTP client resources.

    Attributes:
        auth_provider (StackitAuthProvider): Provider for STACKIT authorization tokens.
        topic_id (str): The topic identifier.
        auth_config (AuthConfig): The authentication configuration.
        base_url (str): The dataplane base URL.
    """

    def __init__(
        self,
        topic_id: str,
        auth_config: AuthConfig | None = None,
        base_url: str = "https://{topicId}.pubsub.eu01.onstackit.cloud",
    ) -> None:
        self.topic_id: str = topic_id
        self.auth_config: AuthConfig = auth_config or AuthConfig()
        self.auth_provider: StackitAuthProvider = StackitAuthProvider(self.auth_config)
        self.base_url: str = base_url.format(topicId=topic_id)

    def get_publisher(self, **kwargs: Any) -> PubSubPublisher:
        """Create a new Publisher instance for this topic.

        Args:
            **kwargs: Additional keyword arguments passed to PubSubPublisher.

        Returns:
            A configured PubSubPublisher instance.
        """
        return PubSubPublisher(
            topic_id=self.topic_id,
            base_url=self.base_url,
            auth_provider=self.auth_provider,
            **kwargs,
        )

    def get_subscriber(self, subscription_id: str, **kwargs: Any) -> PubSubSubscriber:
        """Create a new Subscriber instance for a subscription.

        Args:
            subscription_id: The subscription identifier (UUID).
            **kwargs: Additional keyword arguments passed to PubSubSubscriber.

        Returns:
            A configured PubSubSubscriber instance.
        """
        return PubSubSubscriber(
            subscription_id=subscription_id,
            base_url=self.base_url,
            auth_provider=self.auth_provider,
            **kwargs,
        )

    async def close(self) -> None:
        """Release any shared resources.

        PubSubClient itself holds no HTTP connections; publishers and
        subscribers created via get_publisher()/get_subscriber() own their
        own http_client and must be closed individually (or via
        `async with`).
        """
        return None


__all__ = [
    "PubSubClient",
]
