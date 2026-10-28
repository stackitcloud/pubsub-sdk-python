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

"""Tests for retry logic."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pubsub_sdk_python.errors import (
    PubSubRetryError,
    PubSubServerError,
    PubSubValidationError,
)
from pubsub_sdk_python.models import PublishMessage
from pubsub_sdk_python.publisher import PubSubPublisher
from pubsub_sdk_python.retry import (
    RetryConfig,
    _calculate_delay,
    _is_retryable_status,
    get_retry_delay,
    with_async_retry,
    with_retry,
)


class TestRetryConfig:
    """Test RetryConfig defaults."""

    def test_default_values(self):
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 0.5
        assert config.max_delay == 30.0
        assert config.multiplier == 2.0

    def test_custom_values(self):
        config = RetryConfig(
            max_retries=5,
            base_delay=1.0,
            max_delay=60.0,
            multiplier=3.0,
        )
        assert config.max_retries == 5
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.multiplier == 3.0


class TestRetryableStatus:
    """Test _is_retryable_status function."""

    def test_500_is_retryable(self):
        assert _is_retryable_status(500) is True

    def test_502_is_retryable(self):
        assert _is_retryable_status(502) is True

    def test_503_is_retryable(self):
        assert _is_retryable_status(503) is True

    def test_504_is_retryable(self):
        assert _is_retryable_status(504) is True

    def test_200_is_not_retryable(self):
        assert _is_retryable_status(200) is False

    def test_400_is_not_retryable(self):
        assert _is_retryable_status(400) is False

    def test_401_is_not_retryable(self):
        assert _is_retryable_status(401) is False

    def test_404_is_not_retryable(self):
        assert _is_retryable_status(404) is False


class TestCalculateDelay:
    """Test exponential backoff delay calculation."""

    def test_first_attempt(self):
        config = RetryConfig(base_delay=0.5, multiplier=2.0, max_delay=30.0)
        delay = _calculate_delay(config, 0)
        assert delay >= 0.5  # Base delay + jitter

    def test_second_attempt_exponential(self):
        config = RetryConfig(base_delay=0.5, multiplier=2.0, max_delay=30.0)
        delay = _calculate_delay(config, 1)
        assert delay >= 1.0  # 0.5 * 2^1

    def test_max_delay_cap(self):
        config = RetryConfig(base_delay=0.5, multiplier=2.0, max_delay=2.0)
        delay = _calculate_delay(config, 10)
        assert delay <= 2.5  # max_delay + max jitter (25%)

    def test_delay_increases_with_attempt(self):
        config = RetryConfig(base_delay=0.5, multiplier=2.0, max_delay=30.0)
        delays = [_calculate_delay(config, i) for i in range(5)]
        for i in range(1, len(delays)):
            assert delays[i] > delays[i - 1]


class TestWithRetrySync:
    """Test synchronous with_retry decorator."""

    def test_sync_success_on_first_try(self):
        call_count = 0

        @with_retry
        def my_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = my_func()
        assert result == "success"
        assert call_count == 1

    def test_sync_retry_then_success(self):
        call_count = 0

        @with_retry
        def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise PubSubServerError("Server error")
            return "success"

        result = my_func()
        assert result == "success"
        assert call_count == 3

    def test_sync_no_retry_for_non_retryable(self):
        call_count = 0

        @with_retry
        def my_func():
            nonlocal call_count
            call_count += 1
            raise PubSubValidationError("Validation error")

        with pytest.raises(PubSubValidationError):
            my_func()
        assert call_count == 1

    def test_sync_exhausts_retries(self):
        call_count = 0

        @with_retry
        def my_func():
            nonlocal call_count
            call_count += 1
            raise PubSubServerError("Server error")

        with pytest.raises(PubSubRetryError):
            my_func()
        assert call_count == 4  # 1 initial + 3 retries

    def test_sync_with_custom_config(self):
        call_count = 0

        @with_retry(config=RetryConfig(max_retries=1, base_delay=0.01, max_delay=0.02))
        def my_func():
            nonlocal call_count
            call_count += 1
            raise PubSubServerError("Server error")

        with pytest.raises(PubSubRetryError, match="All 1 retry attempts exhausted: Server error"):
            my_func()
        assert call_count == 2


@pytest.mark.asyncio
class TestWithAsyncRetry:
    """Test async with_async_retry decorator."""

    async def test_async_success_on_first_try(self):
        call_count = 0

        @with_async_retry
        async def my_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await my_func()
        assert result == "success"
        assert call_count == 1

    async def test_async_retry_then_success(self):
        call_count = 0

        @with_async_retry
        async def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise PubSubServerError("Server error")
            return "success"

        result = await my_func()
        assert result == "success"
        assert call_count == 3

    async def test_async_exhausts_retries(self):
        call_count = 0

        @with_async_retry
        async def my_func():
            nonlocal call_count
            call_count += 1
            raise PubSubServerError("Server error")

        with pytest.raises(PubSubRetryError):
            await my_func()
        assert call_count == 4

    async def test_async_with_custom_config(self):
        call_count = 0

        @with_async_retry(config=RetryConfig(max_retries=1, base_delay=0.01, max_delay=0.02))
        async def my_func():
            nonlocal call_count
            call_count += 1
            raise PubSubServerError("Server error")

        with pytest.raises(PubSubRetryError, match="All 1 retry attempts exhausted: Server error"):
            await my_func()
        assert call_count == 2


@pytest.mark.asyncio
class TestPublisherRetryIntegration:
    """Test retry integration with Publisher."""

    def _make_publisher(self, auth_provider, side_effect, retry_config=None):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(side_effect=side_effect)
        mock_client.aclose = AsyncMock()

        ok_response = MagicMock(spec=httpx.Response)
        ok_response.status_code = 204
        ok_response.text = ""

        if retry_config is None:
            retry_config = RetryConfig(
                max_retries=3,
                base_delay=0.01,
                max_delay=0.05,
                multiplier=1.0,
                retryable_exceptions=(PubSubServerError,),
                retryable_status_codes={500, 502, 503, 504},
            )

        return PubSubPublisher(
            topic_id="test-topic",
            base_url="https://test.pubsub.eu01.onstackit.cloud",
            auth_provider=auth_provider,
            retry_config=retry_config,
            http_client=mock_client,
        )

    async def test_publisher_retries_on_server_error(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 503
        error_response.text = "Service Unavailable"

        ok_response = MagicMock(spec=httpx.Response)
        ok_response.status_code = 204
        ok_response.text = ""

        publisher = self._make_publisher(
            auth_provider,
            side_effect=[error_response, error_response, ok_response],
        )

        result = await publisher.publish([PublishMessage.from_plain("test")])
        assert result is None
        assert publisher.http_client.request.call_count == 3

    async def test_publisher_no_retry_on_validation_error(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 400
        error_response.text = "Bad request"

        publisher = self._make_publisher(auth_provider, side_effect=[error_response])

        with pytest.raises(PubSubValidationError):
            await publisher.publish([PublishMessage.from_plain("test")])

        assert publisher.http_client.request.call_count == 1

    async def test_publisher_retry_exhausted(self, auth_provider):
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 503
        error_response.text = "Service Unavailable"

        publisher = self._make_publisher(
            auth_provider,
            side_effect=[error_response] * 10,
            retry_config=RetryConfig(
                max_retries=2,
                base_delay=0.01,
                max_delay=0.05,
                multiplier=1.0,
                retryable_exceptions=(PubSubServerError,),
                retryable_status_codes={503},
            ),
        )

        with pytest.raises(PubSubRetryError):
            await publisher.publish([PublishMessage.from_plain("test")])

        assert publisher.http_client.request.call_count == 3


class TestGetRetryDelay:
    """Test public helper function."""

    def test_returns_positive_delay(self):
        config = RetryConfig(base_delay=0.5, multiplier=2.0, max_delay=30.0)
        delay = get_retry_delay(config, 0)
        assert delay > 0

    def test_delay_increases(self):
        config = RetryConfig(base_delay=1.0, multiplier=2.0, max_delay=60.0)
        d0 = get_retry_delay(config, 0)
        d1 = get_retry_delay(config, 1)
        d2 = get_retry_delay(config, 2)
        assert d0 < d1 < d2
