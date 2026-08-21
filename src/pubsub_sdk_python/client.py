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
        auth_provider: Provider for STACKIT authorization tokens.
        topic_id: The topic identifier.
        base_url: The dataplane base URL.
    """

    def __init__(
        self,
        topic_id: str,
        auth_config: AuthConfig | None = None,
        base_url: str = "https://{topicId}.pubsub.eu01.onstackit.cloud",
    ) -> None:
        self.topic_id = topic_id
        self.auth_config = auth_config or AuthConfig()
        self.auth_provider = StackitAuthProvider(self.auth_config)
        self.base_url = base_url.format(topicId=topic_id)

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
