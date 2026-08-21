"""Subscriber interface for receiving messages from STACKIT PubSub subscriptions."""

from __future__ import annotations

import asyncio
import logging
import types
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
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
    Message,
)
from pubsub_sdk_python.retry import (
    RetryConfig,
    _calculate_delay,
    _is_retryable_status,
)

logger = logging.getLogger(__name__)

# Default number of messages to pull in a single pull request
DEFAULT_PULL_MAX_MESSAGES = 16
# Default long-pull duration in milliseconds
DEFAULT_PULL_LONG_PULL_DURATION_MS = 5000
# Minimum and maximum for pull max messages
MIN_PULL_MAX_MESSAGES = 1
MAX_PULL_MAX_MESSAGES = 32
# Minimum and maximum for pull long-pull duration (ms)
MIN_PULL_LONG_PULL_DURATION_MS = 100
MAX_PULL_LONG_PULL_DURATION_MS = 5000


@dataclass
class PubSubSubscriber:
    """Subscriber interface for receiving messages from a STACKIT PubSub subscription.

    Provides methods to pull, acknowledge, and negatively acknowledge messages,
    with built-in retry logic for transient errors.

    Attributes:
        subscription_id: The subscription identifier (UUID).
        base_url: The dataplane base URL.
        auth_provider: Provider for STACKIT authorization tokens.
        retry_config: Configuration for retry behavior.
        http_client: Shared HTTP client instance.
        _messages_received: Total count of messages pulled.
        _messages_acked: Total count of messages acknowledged.
        _messages_nacked: Total count of messages negatively acknowledged.
        _pull_calls: Total count of pull API calls made.
        _ack_calls: Total count of ack API calls made.
        _nack_calls: Total count of nack API calls made.
    """

    subscription_id: str
    base_url: str
    auth_provider: StackitAuthProvider
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    http_client: httpx.AsyncClient = field(default_factory=httpx.AsyncClient)
    _messages_received: int = 0
    _messages_acked: int = 0
    _messages_nacked: int = 0
    _pull_calls: int = 0
    _ack_calls: int = 0
    _nack_calls: int = 0

    def __post_init__(self):
        if not self.base_url.endswith("/"):
            self.base_url = self.base_url + "/"

    @property
    def messages_received(self) -> int:
        """Total number of messages pulled via this subscriber."""
        return self._messages_received

    @property
    def messages_acked(self) -> int:
        """Total number of messages acknowledged via this subscriber."""
        return self._messages_acked

    @property
    def messages_nacked(self) -> int:
        """Total number of messages negatively acknowledged via this subscriber."""
        return self._messages_nacked

    @property
    def pull_calls(self) -> int:
        """Total number of pull API calls made."""
        return self._pull_calls

    @property
    def ack_calls(self) -> int:
        """Total number of ack API calls made."""
        return self._ack_calls

    @property
    def nack_calls(self) -> int:
        """Total number of nack API calls made."""
        return self._nack_calls

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
            PubSubAuthenticationError: On 401/403 errors.
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
                if response.status_code == 403:
                    from pubsub_sdk_python.errors import PubSubAuthenticationError

                    raise PubSubAuthenticationError(f"Forbidden: {response.text}")

                if _is_retryable_status(response.status_code):
                    raise PubSubServerError(f"Server error {response.status_code}: {response.text}")

                raise PubSubError(f"Unexpected error {response.status_code}: {response.text}")

            except Exception as exc:
                last_exception = exc

                if not isinstance(exc, self.retry_config.retryable_exceptions):
                    raise

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

    async def pull(
        self,
        *,
        max_messages: int | None = None,
        long_pull_duration_ms: int | None = None,
    ) -> list[Message]:
        """Pull messages from the subscription.

        Args:
            max_messages: Maximum number of messages to pull (1-32). Defaults to 16.
            long_pull_duration_ms: Long-pull duration in milliseconds (100-5000). Defaults to 5000.

        Returns:
            List of Message instances received.

        Raises:
            PubSubValidationError: If parameters are out of range.
            PubSubRetryError: If all retries are exhausted.
        """
        # Validate max_messages
        if max_messages is None:
            max_messages = DEFAULT_PULL_MAX_MESSAGES
        else:
            if max_messages < MIN_PULL_MAX_MESSAGES or max_messages > MAX_PULL_MAX_MESSAGES:
                raise PubSubValidationError(
                    f"max_messages must be between "
                    f"{MIN_PULL_MAX_MESSAGES} and {MAX_PULL_MAX_MESSAGES}"
                )

        # Validate long_pull_duration_ms
        if long_pull_duration_ms is None:
            long_pull_duration_ms = DEFAULT_PULL_LONG_PULL_DURATION_MS
        else:
            if (
                long_pull_duration_ms < MIN_PULL_LONG_PULL_DURATION_MS
                or long_pull_duration_ms > MAX_PULL_LONG_PULL_DURATION_MS
            ):
                raise PubSubValidationError(
                    f"long_pull_duration_ms must be between "
                    f"{MIN_PULL_LONG_PULL_DURATION_MS} and "
                    f"{MAX_PULL_LONG_PULL_DURATION_MS}"
                )

        self._pull_calls += 1

        headers = {
            "PubSub-Max-Messages": str(max_messages),
            "PubSub-Long-Pull-Duration": str(long_pull_duration_ms),
        }

        path = f"v1alpha/subscriptions/{self.subscription_id}/pull"
        response = await self._request("POST", path, headers=headers)

        data = response.json()
        raw_messages = data.get("messages", [])

        messages: list[Message] = []
        for raw in raw_messages:
            msg = Message(
                id=raw["id"],
                ack_id=raw["ackId"],
                data=raw["data"],
                create_time=_parse_datetime(raw["createTime"]),
                delivery_attempts=raw.get("deliveryAttempts", 1),
            )
            messages.append(msg)

        self._messages_received += len(messages)
        return messages

    async def pull_stream(
        self,
        *,
        max_messages: int | None = None,
        long_pull_duration_ms: int | None = None,
        on_message: Callable[[Message], None] | None = None,
    ) -> list[Message]:
        """Pull messages and process them via callback or accumulate.

        If on_message is provided, it is called for each message received.
        Otherwise, all pulled messages are accumulated and returned.

        Args:
            max_messages: Maximum messages per pull request.
            long_pull_duration_ms: Long-pull duration in milliseconds.
            on_message: Optional callback invoked with each Message.

        Returns:
            All accumulated messages (also passed to on_message if provided).
        """
        messages = await self.pull(
            max_messages=max_messages,
            long_pull_duration_ms=long_pull_duration_ms,
        )

        if on_message is not None:
            for msg in messages:
                on_message(msg)

        return messages

    async def ack(self, ack_ids: list[str]) -> None:
        """Acknowledge messages.

        Args:
            ack_ids: List of ack IDs to acknowledge.

        Raises:
            PubSubValidationError: If the list is empty.
            PubSubRetryError: If all retries are exhausted.
        """
        if not ack_ids:
            raise PubSubValidationError("Cannot ack an empty list of ack IDs")

        self._ack_calls += 1

        request_body = {"ackIds": ack_ids}
        path = f"v1alpha/subscriptions/{self.subscription_id}/ack"
        await self._request("POST", path, json_data=request_body)
        self._messages_acked += len(ack_ids)

    async def nack(self, nack_ids: list[str]) -> None:
        """Negatively acknowledge messages.

        Args:
            nack_ids: List of nack IDs to negatively acknowledge.

        Raises:
            PubSubValidationError: If the list is empty.
            PubSubRetryError: If all retries are exhausted.
        """
        if not nack_ids:
            raise PubSubValidationError("Cannot nack an empty list of nack IDs")

        self._nack_calls += 1

        request_body = {"nackIds": nack_ids}
        path = f"v1alpha/subscriptions/{self.subscription_id}/nack"
        await self._request("POST", path, json_data=request_body)
        self._messages_nacked += len(nack_ids)

    async def ack_messages(self, messages: list[Message]) -> None:
        """Acknowledge a list of Message objects.

        Convenience method that extracts ack IDs from messages.

        Args:
            messages: List of Message instances to acknowledge.
        """
        if not messages:
            return
        await self.ack([msg.ack_id for msg in messages])

    async def nack_messages(self, messages: list[Message]) -> None:
        """Negatively acknowledge a list of Message objects.

        Convenience method that extracts ack IDs from messages.

        Args:
            messages: List of Message instances to negatively acknowledge.
        """
        if not messages:
            return
        await self.nack([msg.ack_id for msg in messages])

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.http_client.aclose()

    async def __aenter__(self) -> PubSubSubscriber:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.close()


def _parse_datetime(dt_str: str) -> datetime:
    """Parse an ISO-8601 datetime string."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


__all__ = [
    "DEFAULT_PULL_LONG_PULL_DURATION_MS",
    "DEFAULT_PULL_MAX_MESSAGES",
    "MAX_PULL_LONG_PULL_DURATION_MS",
    "MAX_PULL_MAX_MESSAGES",
    "MIN_PULL_LONG_PULL_DURATION_MS",
    "MIN_PULL_MAX_MESSAGES",
    "PubSubSubscriber",
]
