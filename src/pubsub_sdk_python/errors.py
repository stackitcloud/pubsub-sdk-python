"""Custom exception hierarchy for the PubSub SDK."""

from __future__ import annotations


class PubSubError(Exception):
    """Base exception for all PubSub SDK errors."""


class PubSubAuthenticationError(PubSubError):
    """Raised when authentication or authorization fails."""


class PubSubValidationError(PubSubError):
    """Raised when a request fails validation (400, 413)."""


class PubSubNotFoundError(PubSubError):
    """Raised when a resource is not found (404)."""


class PubSubServerError(PubSubError):
    """Raised when the server returns a 5xx error."""


class PubSubRetryError(PubSubError):
    """Raised when all retry attempts have been exhausted."""

    def __init__(
        self, message: str = "All retry attempts exhausted", causes: list[Exception] | None = None
    ):
        super().__init__(message)
        self.causes = causes or []


class PubSubTimeoutError(PubSubError, TimeoutError):
    """Raised when a request times out."""
