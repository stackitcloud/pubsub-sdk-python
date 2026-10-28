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

"""Retry logic with exponential backoff for transient errors."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar, overload

import httpx

from pubsub_sdk_python.errors import (
    PubSubRetryError,
    PubSubServerError,
    PubSubTimeoutError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# HTTP status codes that are considered transient and worth retrying
TRANSIENT_STATUS_CODES: set[int] = {500, 502, 503, 504}

# HTTP status codes that are never retried (permanent failures)
NON_RETRYABLE_STATUS_CODES: set[int] = {400, 401, 403, 404, 413}


def _is_retryable_status(code: int) -> bool:
    """Determine if an HTTP status code indicates a transient error."""
    return code in TRANSIENT_STATUS_CODES


# Retryable exceptions - only these get retried
RETRYABLE_EXCEPTIONS = (
    TimeoutError,
    ConnectionError,
    OSError,
    PubSubTimeoutError,
    PubSubServerError,
    # httpx raises its own exception hierarchy that doesn't subclass the
    # builtins above, so transient network failures must be listed explicitly.
    httpx.TimeoutException,  # ConnectTimeout, ReadTimeout, WriteTimeout, PoolTimeout
    httpx.NetworkError,  # ConnectError, ReadError, WriteError, CloseError
)


class RetryConfig:
    """Configuration for retry behavior.

    Attributes:
        max_retries (int): Maximum number of retry attempts (0 = no retries).
        base_delay (float): Base delay in seconds between retries.
        max_delay (float): Maximum delay in seconds between retries.
        multiplier (float): Multiplier applied to delay after each retry (exponential backoff).
        retryable_exceptions (tuple[type[Exception], ...]): Exception types that trigger a retry.
        retryable_status_codes (set[int]): HTTP status codes that trigger a retry.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        multiplier: float = 2.0,
        retryable_exceptions: tuple[type[Exception], ...] | None = None,
        retryable_status_codes: set[int] | None = None,
    ):
        self.max_retries: int = max_retries
        self.base_delay: float = base_delay
        self.max_delay: float = max_delay
        self.multiplier: float = multiplier
        self.retryable_exceptions: tuple[type[Exception], ...] = (
            retryable_exceptions or RETRYABLE_EXCEPTIONS
        )
        self.retryable_status_codes: set[int] = retryable_status_codes or TRANSIENT_STATUS_CODES


def _calculate_delay(config: RetryConfig, attempt: int) -> float:
    """Calculate the delay for a given retry attempt using exponential backoff with jitter.

    Uses the formula: min(base_delay * multiplier^attempt, max_delay)
    Then adds jitter (up to 25% randomization) to avoid thundering herd.
    """
    delay = min(
        config.base_delay * (config.multiplier**attempt),
        config.max_delay,
    )
    # Add jitter: random value between 0 and delay * 0.25
    jitter = delay * 0.25 * (hash((attempt, time.monotonic_ns())) % 100) / 100
    return delay + jitter


def _should_retry(
    exc: Exception,
    status_code: int | None = None,
    *,
    config: RetryConfig | None = None,
) -> bool:
    """Determine whether an exception or status code is retryable.

    Args:
        exc: The exception to check.
        status_code: Optional HTTP status code to check.
        config: Optional RetryConfig to use.

    Returns:
        True if the exception/status code is retryable.
    """
    retryable_exceptions = (
        config.retryable_exceptions if config is not None else RETRYABLE_EXCEPTIONS
    )
    if isinstance(exc, retryable_exceptions):
        return True
    if status_code is not None:
        retryable_codes = (
            config.retryable_status_codes if config is not None else TRANSIENT_STATUS_CODES
        )
        return status_code in retryable_codes
    return False


@overload
def with_retry(func: Callable[..., T]) -> Callable[..., T]: ...


@overload
def with_retry(
    func: None = None, *, config: RetryConfig | None = None
) -> Callable[[Callable[..., T]], Callable[..., T]]: ...


def with_retry(
    func: Callable[..., T] | None = None, *, config: RetryConfig | None = None
) -> Callable[..., T] | Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that adds retry logic with exponential backoff to a synchronous function.

    Args:
        func: The synchronous function to wrap.
        config: Optional RetryConfig to use.

    Returns:
        The wrapped function with retry logic.

    Example:
        @with_retry
        def my_api_call():
            ...  # Will retry on transient errors
    """
    retry_config = config or RetryConfig()

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None
            for attempt in range(retry_config.max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    if not _should_retry(exc, config=retry_config):
                        raise
                    if attempt >= retry_config.max_retries:
                        raise PubSubRetryError(
                            f"All {retry_config.max_retries} retry attempts exhausted: {exc}",
                            causes=[last_exception] if last_exception else None,
                        ) from exc
                    delay = _calculate_delay(retry_config, attempt)
                    logger.warning(
                        "Retry attempt %d/%d for %s after %.2fs: %s",
                        attempt + 1,
                        retry_config.max_retries,
                        getattr(fn, "__name__", repr(fn)),
                        delay,
                        exc,
                    )
                    time.sleep(delay)
            raise PubSubRetryError("Retry loop terminated unexpectedly")

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


@overload
def with_async_retry(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]: ...


@overload
def with_async_retry(
    func: None = None, *, config: RetryConfig | None = None
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]: ...


def with_async_retry(
    func: Callable[..., Awaitable[T]] | None = None, *, config: RetryConfig | None = None
) -> (
    Callable[..., Awaitable[T]]
    | Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]
):
    """Async-friendly retry decorator.

    Adds retry logic with exponential backoff to an async function.

    Args:
        func: The async function to wrap.
        config: Optional RetryConfig to use.

    Returns:
        The wrapped async function with retry logic.

    Example:
        @with_async_retry
        async def my_async_api_call():
            ...  # Will retry on transient errors
    """
    retry_config = config or RetryConfig()

    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None
            for attempt in range(retry_config.max_retries + 1):
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    if not _should_retry(exc, config=retry_config):
                        raise
                    if attempt >= retry_config.max_retries:
                        raise PubSubRetryError(
                            f"All {retry_config.max_retries} retry attempts exhausted: {exc}",
                            causes=[last_exception] if last_exception else None,
                        ) from exc
                    delay = _calculate_delay(retry_config, attempt)
                    logger.warning(
                        "Retry attempt %d/%d for %s after %.2fs: %s",
                        attempt + 1,
                        retry_config.max_retries,
                        getattr(fn, "__name__", repr(fn)),
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)
            raise PubSubRetryError("Retry loop terminated unexpectedly")

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


def get_retry_delay(config: RetryConfig, attempt: int) -> float:
    """Public helper to calculate retry delay outside of a retry context."""
    return _calculate_delay(config, attempt)


__all__ = [
    "NON_RETRYABLE_STATUS_CODES",
    "RETRYABLE_EXCEPTIONS",
    "TRANSIENT_STATUS_CODES",
    "RetryConfig",
    "get_retry_delay",
    "with_async_retry",
    "with_retry",
]
