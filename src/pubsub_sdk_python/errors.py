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
