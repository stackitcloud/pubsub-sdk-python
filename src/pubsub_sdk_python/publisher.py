"""Publisher interface for sending messages to STACKIT PubSub topics."""

from __future__ import annotations

import asyncio
import logging
import types
from dataclasses import dataclass, field
from typing import Any

import httpx

from pubsub_sdk_python.auth import StackitAuthProvider
from pubsub_sdk_python.errors import (
    PubSubError,
    PubSubNotFoundError,
    PubSubRetryError,
    PubSubServerError,
    PubSubValidationError,
)
from pubsub_sdk_python.models import (
    PublishMessage,
    PublishMessageResponse,
)
from pubsub_sdk_python.retry import (
    RetryConfig,
    _calculate_delay,
    _is_retryable_status,
)

logger = logging.getLogger(__name__)


@dataclass
class PubSubPublisher:
    """Publisher interface for streaming payloads to a STACKIT PubSub topic.

    Provides methods to publish messages and purge topics, with built-in
    retry logic for transient errors.

    Attributes:
        topic_id: The topic identifier (UUID).
        base_url: The dataplane base URL.
        auth_provider: Provider for STACKIT authorization tokens.
        retry_config: Configuration for retry behavior.
        http_client: Shared HTTP client instance.
        _messages_sent: Total count of messages sent.
        _publish_calls: Total count of publish calls made.
        _purge_calls: Total count of purge calls made.
        _messages_purged: Count of messages purged (when available).
    """

    topic_id: str
    base_url: str
    auth_provider: StackitAuthProvider
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    http_client: httpx.AsyncClient = field(default_factory=httpx.AsyncClient)
    _messages_sent: int = 0
    _publish_calls: int = 0
    _purge_calls: int = 0
    _messages_purged: int = 0

    def __post_init__(self):
        if not self.base_url.endswith("/"):
            self.base_url = self.base_url + "/"

    @property
    def messages_sent(self) -> int:
        """Total number of messages sent via this publisher."""
        return self._messages_sent

    @property
    def publish_calls(self) -> int:
        """Total number of publish API calls made."""
        return self._publish_calls

    @property
    def purge_calls(self) -> int:
        """Total number of purge API calls made."""
        return self._purge_calls

    @property
    def messages_purged(self) -> int:
        """Total number of messages purged."""
        return self._messages_purged

    def _build_auth_headers(self) -> dict[str, str]:
        """Build headers including authentication."""
        return self.auth_provider.get_auth_header()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Execute an HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: URL path (relative to base_url).
            json_data: Optional JSON request body.
            headers: Optional additional headers.
            params: Optional query parameters.

        Returns:
            The HTTP response.

        Raises:
            PubSubValidationError: On 400/413 errors.
            PubSubNotFoundError: On 404 errors.
            PubSubAuthenticationError: On 401 errors.
            PubSubRetryError: When all retries are exhausted for transient errors.
        """
        last_exception: Exception | None = None

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                url = f"{self.base_url}{path}"
                request_headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    **self._build_auth_headers(),
                }
                if headers:
                    request_headers.update(headers)

                response = await self.http_client.request(
                    method,
                    url,
                    json=json_data,
                    headers=request_headers,
                    params=params,
                    timeout=30.0,
                )

                # Handle response status codes
                if response.status_code == 200 or response.status_code == 204:
                    return response

                if response.status_code == 400:
                    raise PubSubValidationError(f"Bad request: {response.text}")
                if response.status_code == 413:
                    raise PubSubValidationError(f"Request body too large: {response.text}")
                if response.status_code == 404:
                    raise PubSubNotFoundError(f"Resource not found: {response.text}")
                if response.status_code == 401:
                    from pubsub_sdk_python.errors import PubSubAuthenticationError

                    raise PubSubAuthenticationError(f"Unauthorized: {response.text}")

                # Transient error - may retry
                if _is_retryable_status(response.status_code):
                    raise PubSubServerError(f"Server error {response.status_code}: {response.text}")

                # Any other error status - no retry
                raise PubSubError(f"Unexpected error {response.status_code}: {response.text}")

            except Exception as exc:
                last_exception = exc

                # Don't retry non-retryable errors
                if not isinstance(exc, self.retry_config.retryable_exceptions):
                    raise

                # If we've exhausted retries
                if attempt >= self.retry_config.max_retries:
                    raise PubSubRetryError(
                        f"All {self.retry_config.max_retries} retry attempts exhausted for {path}",
                        causes=[last_exception] if last_exception else None,
                    ) from exc

                delay = _calculate_delay(self.retry_config, attempt)
                logger.warning(
                    "Retry attempt %d/%d for %s %s after %.2fs: %s",
                    attempt + 1,
                    self.retry_config.max_retries,
                    method,
                    path,
                    delay,
                    exc,
                )
                await self._safe_sleep(delay)

        raise last_exception  # type: ignore[misc]

    @staticmethod
    async def _safe_sleep(delay: float) -> None:
        """Safe async sleep for retry backoff."""
        await asyncio.sleep(delay)

    async def publish(self, messages: list[PublishMessage]) -> PublishMessageResponse:
        """Publish messages to the topic.

        Args:
            messages: List of PublishMessage instances to send.

        Returns:
            PublishMessageResponse with the assigned message IDs.

        Raises:
            PubSubValidationError: If the request is invalid.
            PubSubRetryError: If all retries are exhausted.
        """
        if not messages:
            raise PubSubValidationError("Cannot publish an empty list of messages")

        if len(messages) > 100:
            raise PubSubValidationError("Maximum 100 messages per publish request")

        self._publish_calls += 1
        self._messages_sent += len(messages)

        request_body = {"messages": [{"data": msg.data} for msg in messages]}

        response = await self._request(
            "POST",
            "v1alpha/publish",
            json_data=request_body,
        )

        data = response.json()
        return PublishMessageResponse(message_ids=data.get("messageIds", []))

    async def publish_batch(
        self,
        messages: list[PublishMessage],
        batch_size: int = 100,
    ) -> list[PublishMessageResponse]:
        """Publish messages in batches.

        Splits the message list into batches and publishes each one sequentially.

        Args:
            messages: All messages to publish.
            batch_size: Maximum messages per batch.

        Returns:
            List of PublishMessageResponse objects, one per batch.
        """
        if not messages:
            return []

        responses: list[PublishMessageResponse] = []
        for i in range(0, len(messages), batch_size):
            batch = messages[i : i + batch_size]
            response = await self.publish(batch)
            responses.append(response)
        return responses

    async def purge_topic(self) -> None:
        """Purge all messages from the topic.

        This operation permanently deletes all messages in the topic and
        cannot be undone.

        Raises:
            PubSubRetryError: If all retries are exhausted.
        """
        self._purge_calls += 1
        await self._request("POST", "v1alpha/purge")
        self._messages_purged = -1  # -1 indicates full purge (exact count unknown)

    async def purge_topic_with_confirm(self, confirm: str = "purge") -> dict[str, Any]:
        """Purge all messages from the topic with a confirmation value.

        Args:
            confirm: Confirmation string that must equal 'purge' to proceed.

        Returns:
            Status dictionary confirming the purge operation.
        """
        if confirm != "purge":
            raise PubSubValidationError("Confirmation value must be 'purge'")

        await self.purge_topic()
        return {
            "status": "purged",
            "topic_id": self.topic_id,
            "messages": "all",
        }

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.http_client.aclose()

    async def __aenter__(self) -> PubSubPublisher:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.close()


__all__ = [
    "PubSubPublisher",
]
