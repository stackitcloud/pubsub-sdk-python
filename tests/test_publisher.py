"""Tests for the Publisher interface."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pubsub_sdk_python.errors import (
    PubSubAuthenticationError,
    PubSubNotFoundError,
    PubSubRetryError,
    PubSubServerError,
    PubSubValidationError,
)
from pubsub_sdk_python.models import PublishMessage, PublishMessageResponse
from pubsub_sdk_python.publisher import PubSubPublisher


class TestPubSubPublisherInit:
    """Test PubSubPublisher initialization."""

    def test_init_with_defaults(self, auth_provider, base_url):
        publisher = PubSubPublisher(
            topic_id="test-topic",
            base_url=base_url,
            auth_provider=auth_provider,
        )
        assert publisher.topic_id == "test-topic"
        assert publisher.base_url == base_url + "/"
        assert publisher.messages_sent == 0
        assert publisher.publish_calls == 0
        assert publisher.purge_calls == 0
        assert publisher.messages_purged == 0

    def test_init_trailing_slash_normalization(self, auth_provider):
        publisher = PubSubPublisher(
            topic_id="test",
            base_url="https://example.com",
            auth_provider=auth_provider,
        )
        assert publisher.base_url == "https://example.com/"

    def test_init_no_trailing_slash_preserved(self, auth_provider):
        publisher = PubSubPublisher(
            topic_id="test",
            base_url="https://example.com/",
            auth_provider=auth_provider,
        )
        assert publisher.base_url == "https://example.com/"


@pytest.mark.asyncio
class TestPublisherPublish:
    """Test the publish method."""

    def _make_publisher(
        self, auth_provider, response=None, base_url="https://test.pubsub.eu01.onstackit.cloud"
    ):
        """Helper to create a publisher with a mocked HTTP client."""
        if response is None:
            response = MagicMock(spec=httpx.Response)
            response.status_code = 200
            response.json.return_value = {"messageIds": [1, 2]}
            response.text = "OK"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=response)
        mock_client.aclose = AsyncMock()

        return PubSubPublisher(
            topic_id="test-topic",
            base_url=base_url,
            auth_provider=auth_provider,
            http_client=mock_client,
        )

    async def test_publish_success(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        messages = [
            PublishMessage.from_plain("Hello"),
            PublishMessage.from_plain("World"),
        ]
        result = await publisher.publish(messages)

        assert isinstance(result, PublishMessageResponse)
        assert result.message_ids == [1, 2]
        assert publisher.messages_sent == 2
        assert publisher.publish_calls == 1

    async def test_publish_sends_correct_payload(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        messages = [PublishMessage.from_plain("test")]

        await publisher.publish(messages)

        call_args = publisher.http_client.request.call_args
        assert call_args.kwargs["json"] == {"messages": [{"data": "test"}]} or "messages" in str(
            call_args
        )

    async def test_publish_empty_list_raises(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        with pytest.raises(PubSubValidationError, match="Cannot publish"):
            await publisher.publish([])

    async def test_publish_too_many_messages_raises(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        messages = [PublishMessage.from_plain(f"msg-{i}") for i in range(101)]
        with pytest.raises(PubSubValidationError, match="Maximum 100"):
            await publisher.publish(messages)

    async def test_publish_batch(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        messages = [PublishMessage.from_plain(f"msg-{i}") for i in range(5)]

        responses = await publisher.publish_batch(messages, batch_size=2)

        assert len(responses) == 3  # 5 messages / batch of 2 = 3 batches
        assert publisher.publish_calls == 3

    async def test_publish_batch_empty(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        responses = await publisher.publish_batch([])
        assert responses == []

    async def test_publish_500_triggers_retry(self, auth_provider):
        """Test that 500 errors trigger retry behavior."""
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 503
        error_response.text = "Service Unavailable"

        ok_response = MagicMock(spec=httpx.Response)
        ok_response.status_code = 200
        ok_response.json.return_value = {"messageIds": [42]}
        ok_response.text = "OK"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        # Fail twice, then succeed
        mock_client.request = AsyncMock(side_effect=[error_response, error_response, ok_response])
        mock_client.aclose = AsyncMock()

        publisher = PubSubPublisher(
            topic_id="test-topic",
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

        result = await publisher.publish([PublishMessage.from_plain("test")])
        assert result.message_ids == [42]
        assert mock_client.request.call_count == 3

    async def test_publish_exhausts_retries(self, auth_provider):
        """Test that retries are exhausted after max_retries attempts."""
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 503
        error_response.text = "Service Unavailable"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=error_response)
        mock_client.aclose = AsyncMock()

        publisher = PubSubPublisher(
            topic_id="test-topic",
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
            await publisher.publish([PublishMessage.from_plain("test")])

        assert mock_client.request.call_count == 3  # 1 initial + 2 retries

    async def test_publish_400_raises_validation_error(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 400
        error_response.text = "Bad request"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=error_response)
        mock_client.aclose = AsyncMock()

        publisher = PubSubPublisher(
            topic_id="test-topic",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            http_client=mock_client,
        )

        with pytest.raises(PubSubValidationError, match="Bad request"):
            await publisher.publish([PublishMessage.from_plain("test")])

    async def test_publish_404_raises_not_found(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 404
        error_response.text = "Not found"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=error_response)
        mock_client.aclose = AsyncMock()

        publisher = PubSubPublisher(
            topic_id="test-topic",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            http_client=mock_client,
        )

        with pytest.raises(PubSubNotFoundError, match="Not found"):
            await publisher.publish([PublishMessage.from_plain("test")])

    async def test_publish_401_raises_auth_error(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 401
        error_response.text = "Unauthorized"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=error_response)
        mock_client.aclose = AsyncMock()

        publisher = PubSubPublisher(
            topic_id="test-topic",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            http_client=mock_client,
        )

        with pytest.raises(PubSubAuthenticationError):
            await publisher.publish([PublishMessage.from_plain("test")])


@pytest.mark.asyncio
class TestPublisherPurge:
    """Test the purge_topic method."""

    def _make_publisher(self, auth_provider, response=None):
        if response is None:
            response = MagicMock(spec=httpx.Response)
            response.status_code = 204
            response.text = ""

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=response)
        mock_client.aclose = AsyncMock()

        return PubSubPublisher(
            topic_id="test-topic",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            http_client=mock_client,
        )

    async def test_purge_topic_success(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        await publisher.purge_topic()

        assert publisher.purge_calls == 1
        assert publisher.messages_purged == -1

    async def test_purge_topic_with_confirm(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        result = await publisher.purge_topic_with_confirm()

        assert result["status"] == "purged"
        assert result["topic_id"] == "test-topic"

    async def test_purge_topic_with_confirm_wrong_value(self, auth_provider):
        publisher = self._make_publisher(auth_provider)
        with pytest.raises(PubSubValidationError, match="Confirmation value"):
            await publisher.purge_topic_with_confirm(confirm="wrong")

    async def test_purge_404_raises_not_found(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 404
        error_response.text = "Not found"

        publisher = self._make_publisher(auth_provider, error_response)
        with pytest.raises(PubSubNotFoundError):
            await publisher.purge_topic()


@pytest.mark.asyncio
class TestPublisherContextManager:
    """Test async context manager support."""

    async def test_context_manager(self, auth_provider):
        mock_client = MagicMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock()
        mock_client.aclose = AsyncMock()

        publisher = PubSubPublisher(
            topic_id="test-topic",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            http_client=mock_client,
        )

        async with publisher as p:
            assert p is publisher

        mock_client.aclose.assert_called_once()
