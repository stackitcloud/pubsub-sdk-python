"""Tests for the Subscriber interface."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pubsub_python_sdk.errors import (
    PubSubAuthenticationError,
    PubSubNotFoundError,
    PubSubRetryError,
    PubSubServerError,
    PubSubValidationError,
)
from pubsub_python_sdk.models import Message
from pubsub_python_sdk.subscriber import PubSubSubscriber


class TestPubSubSubscriberInit:
    """Test PubSubSubscriber initialization."""

    def test_init_with_defaults(self, auth_provider):
        subscriber = PubSubSubscriber(
            subscription_id="test-sub",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
        )
        assert subscriber.subscription_id == "test-sub"
        assert subscriber.base_url == "https://test.pubsub.eu01.onstackit.cloud/"
        assert subscriber.messages_received == 0
        assert subscriber.messages_acked == 0
        assert subscriber.messages_nacked == 0
        assert subscriber.pull_calls == 0
        assert subscriber.ack_calls == 0
        assert subscriber.nack_calls == 0

    def test_init_trailing_slash_normalization(self, auth_provider):
        subscriber = PubSubSubscriber(
            subscription_id="test",
            base_url="https://example.com",
            auth_provider=auth_provider,
        )
        assert subscriber.base_url == "https://example.com/"


def _make_subscriber(auth_provider, response=None):
    """Helper to create a subscriber with a mocked HTTP client."""
    if response is None:
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = {"messages": []}
        response.text = "OK"

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.request = AsyncMock(return_value=response)
    mock_client.aclose = AsyncMock()

    return PubSubSubscriber(
        subscription_id="test-sub",
        base_url="https://test.pubsub.eu01.onstackit.cloud",
        auth_provider=auth_provider,
        http_client=mock_client,
    )


@pytest.mark.asyncio
class TestSubscriberPull:
    """Test the pull method."""

    async def test_pull_success(self, auth_provider, sample_pull_messages):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = sample_pull_messages
        response.text = "OK"

        subscriber = _make_subscriber(auth_provider, response)
        messages = await subscriber.pull()

        assert len(messages) == 2
        assert isinstance(messages[0], Message)
        assert messages[0].ack_id == "ack-1"
        assert messages[0].id == 1
        assert messages[0].text == "Hello, World!"
        assert subscriber.messages_received == 2
        assert subscriber.pull_calls == 1

    async def test_pull_empty(self, auth_provider):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = {"messages": []}
        response.text = "OK"

        subscriber = _make_subscriber(auth_provider, response)
        messages = await subscriber.pull()

        assert messages == []
        assert subscriber.messages_received == 0

    async def test_pull_with_max_messages(self, auth_provider, sample_pull_messages):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = sample_pull_messages
        response.text = "OK"

        subscriber = _make_subscriber(auth_provider, response)
        await subscriber.pull(max_messages=5)

        call_args = subscriber.http_client.request.call_args
        headers = call_args.kwargs.get("headers", {})
        assert headers.get("PubSub-Max-Messages") == "5"

    async def test_pull_with_long_pull_duration(self, auth_provider, sample_pull_messages):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = sample_pull_messages
        response.text = "OK"

        subscriber = _make_subscriber(auth_provider, response)
        await subscriber.pull(long_pull_duration_ms=3000)

        call_args = subscriber.http_client.request.call_args
        headers = call_args.kwargs.get("headers", {})
        assert headers.get("PubSub-Long-Pull-Duration") == "3000"

    async def test_pull_invalid_max_messages(self, auth_provider):
        subscriber = _make_subscriber(auth_provider)
        with pytest.raises(PubSubValidationError, match="max_messages"):
            await subscriber.pull(max_messages=50)

    async def test_pull_invalid_max_messages_min(self, auth_provider):
        subscriber = _make_subscriber(auth_provider)
        with pytest.raises(PubSubValidationError, match="max_messages"):
            await subscriber.pull(max_messages=0)

    async def test_pull_invalid_long_pull_duration(self, auth_provider):
        subscriber = _make_subscriber(auth_provider)
        with pytest.raises(PubSubValidationError, match="long_pull_duration"):
            await subscriber.pull(long_pull_duration_ms=10000)

    async def test_pull_500_triggers_retry(self, auth_provider):
        """Test that 503 errors trigger retry."""
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 503
        error_response.text = "Service Unavailable"

        ok_response = MagicMock(spec=httpx.Response)
        ok_response.status_code = 200
        ok_response.json.return_value = {"messages": []}
        ok_response.text = "OK"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(side_effect=[error_response, error_response, ok_response])
        mock_client.aclose = AsyncMock()

        subscriber = PubSubSubscriber(
            subscription_id="test-sub",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            retry_config=type(
                "RetryConfig",
                (),
                {
                    "max_retries": 3,
                    "base_delay": 0.01,
                    "max_delay": 0.05,
                    "multiplier": 1.0,
                    "retryable_exceptions": (PubSubServerError,),
                    "retryable_status_codes": {503},
                },
            )(),
            http_client=mock_client,
        )

        await subscriber.pull()
        assert mock_client.request.call_count == 3

    async def test_pull_exhausts_retries(self, auth_provider):
        """Test that retries are exhausted after max_retries."""
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 503
        error_response.text = "Service Unavailable"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=error_response)
        mock_client.aclose = AsyncMock()

        subscriber = PubSubSubscriber(
            subscription_id="test-sub",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            retry_config=type(
                "RetryConfig",
                (),
                {
                    "max_retries": 2,
                    "base_delay": 0.01,
                    "max_delay": 0.05,
                    "multiplier": 1.0,
                    "retryable_exceptions": (PubSubServerError,),
                    "retryable_status_codes": {503},
                },
            )(),
            http_client=mock_client,
        )

        with pytest.raises(PubSubRetryError):
            await subscriber.pull()

        assert mock_client.request.call_count == 3


@pytest.mark.asyncio
class TestSubscriberAck:
    """Test the ack method."""

    async def test_ack_success(self, auth_provider):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 204
        response.text = ""

        subscriber = _make_subscriber(auth_provider, response)
        await subscriber.ack(["ack-1", "ack-2"])

        assert subscriber.ack_calls == 1
        assert subscriber.messages_acked == 2

        call_args = subscriber.http_client.request.call_args
        json_data = call_args.kwargs.get("json")
        assert json_data["ackIds"] == ["ack-1", "ack-2"]

    async def test_ack_empty_list_raises(self, auth_provider):
        subscriber = _make_subscriber(auth_provider)
        with pytest.raises(PubSubValidationError, match="empty"):
            await subscriber.ack([])

    async def test_ack_messages(self, auth_provider):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 204
        response.text = ""

        subscriber = _make_subscriber(auth_provider, response)
        messages = [
            Message(
                id=1,
                ack_id="ack-1",
                data="dGVzdA==",
                create_time=datetime(2024, 1, 1, tzinfo=UTC),
            ),
            Message(
                id=2,
                ack_id="ack-2",
                data="dGVzdA==",
                create_time=datetime(2024, 1, 1, tzinfo=UTC),
            ),
        ]
        await subscriber.ack_messages(messages)

        assert subscriber.messages_acked == 2


@pytest.mark.asyncio
class TestSubscriberNack:
    """Test the nack method."""

    async def test_nack_success(self, auth_provider):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 204
        response.text = ""

        subscriber = _make_subscriber(auth_provider, response)
        await subscriber.nack(["nack-1", "nack-2"])

        assert subscriber.nack_calls == 1
        assert subscriber.messages_nacked == 2

        call_args = subscriber.http_client.request.call_args
        json_data = call_args.kwargs.get("json")
        assert json_data["nackIds"] == ["nack-1", "nack-2"]

    async def test_nack_empty_list_raises(self, auth_provider):
        subscriber = _make_subscriber(auth_provider)
        with pytest.raises(PubSubValidationError, match="empty"):
            await subscriber.nack([])

    async def test_nack_messages(self, auth_provider):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 204
        response.text = ""

        subscriber = _make_subscriber(auth_provider, response)
        messages = [
            Message(
                id=1,
                ack_id="ack-1",
                data="dGVzdA==",
                create_time=datetime(2024, 1, 1, tzinfo=UTC),
            ),
        ]
        await subscriber.nack_messages(messages)

        assert subscriber.messages_nacked == 1

    async def test_nack_messages_empty_list(self, auth_provider):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 204
        response.text = ""

        subscriber = _make_subscriber(auth_provider, response)
        await subscriber.nack_messages([])

        assert subscriber.messages_nacked == 0


@pytest.mark.asyncio
class TestSubscriberPullStream:
    """Test the pull_stream method."""

    async def test_pull_stream_accumulates(self, auth_provider, sample_pull_messages):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = sample_pull_messages
        response.text = "OK"

        subscriber = _make_subscriber(auth_provider, response)
        messages = await subscriber.pull_stream()

        assert len(messages) == 2
        assert subscriber.messages_received == 2

    async def test_pull_stream_with_callback(self, auth_provider, sample_pull_messages):
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = sample_pull_messages
        response.text = "OK"

        received = []
        subscriber = _make_subscriber(auth_provider, response)

        def on_msg(msg):
            received.append(msg)

        messages = await subscriber.pull_stream(on_message=on_msg)

        assert len(received) == 2
        assert len(messages) == 2


@pytest.mark.asyncio
class TestSubscriberContextManager:
    """Test async context manager support."""

    async def test_context_manager(self, auth_provider):
        mock_client = MagicMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock()
        mock_client.aclose = AsyncMock()

        subscriber = PubSubSubscriber(
            subscription_id="test-sub",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            http_client=mock_client,
        )

        async with subscriber as s:
            assert s is subscriber

        mock_client.aclose.assert_called_once()


@pytest.mark.asyncio
class TestSubscriberErrorHandling:
    """Test subscriber error handling."""

    async def test_pull_404_raises_not_found(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 404
        error_response.text = "Not found"

        subscriber = _make_subscriber(auth_provider, error_response)
        with pytest.raises(PubSubNotFoundError, match="Not found"):
            await subscriber.pull()

    async def test_pull_401_raises_auth_error(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 401
        error_response.text = "Unauthorized"

        subscriber = _make_subscriber(auth_provider, error_response)
        with pytest.raises(PubSubAuthenticationError):
            await subscriber.pull()

    async def test_ack_404_raises_not_found(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 404
        error_response.text = "Not found"

        subscriber = _make_subscriber(auth_provider, error_response)
        with pytest.raises(PubSubNotFoundError):
            await subscriber.ack(["ack-1"])

    async def test_nack_404_raises_not_found(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 404
        error_response.text = "Not found"

        subscriber = _make_subscriber(auth_provider, error_response)
        with pytest.raises(PubSubNotFoundError):
            await subscriber.nack(["nack-1"])
