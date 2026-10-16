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

"""STACKIT PubSub Python SDK.

A robust Python client for publishing and subscribing to STACKIT PubSub topics.
"""

from pubsub_sdk_python import auth, client, errors, models, publisher, retry, subscriber
from pubsub_sdk_python.auth import AuthConfig, StackitAuthProvider
from pubsub_sdk_python.client import PubSubClient
from pubsub_sdk_python.errors import (
    PubSubAuthenticationError,
    PubSubError,
    PubSubNotFoundError,
    PubSubRetryError,
    PubSubServerError,
    PubSubTimeoutError,
    PubSubValidationError,
)
from pubsub_sdk_python.models import (
    AckMessagesFromTopicRequest,
    Message,
    Messages,
    NackMessagesFromTopicRequest,
    PublishMessage,
    PublishMessageToTopicRequest,
    decode_message_data,
    decode_message_text,
)
from pubsub_sdk_python.publisher import PubSubPublisher
from pubsub_sdk_python.retry import (
    RETRYABLE_EXCEPTIONS,
    RetryConfig,
    get_retry_delay,
    with_async_retry,
    with_retry,
)
from pubsub_sdk_python.subscriber import PubSubSubscriber

__version__ = "0.1.0"

__all__ = [
    "RETRYABLE_EXCEPTIONS",
    "AckMessagesFromTopicRequest",
    "AuthConfig",
    "Message",
    "Messages",
    "NackMessagesFromTopicRequest",
    "PubSubAuthenticationError",
    "PubSubClient",
    "PubSubError",
    "PubSubNotFoundError",
    "PubSubPublisher",
    "PubSubRetryError",
    "PubSubServerError",
    "PubSubSubscriber",
    "PubSubTimeoutError",
    "PubSubValidationError",
    "PublishMessage",
    "PublishMessageToTopicRequest",
    "RetryConfig",
    "StackitAuthProvider",
    "__version__",
    "auth",
    "client",
    "decode_message_data",
    "decode_message_text",
    "errors",
    "get_retry_delay",
    "models",
    "publisher",
    "retry",
    "subscriber",
    "with_async_retry",
    "with_retry",
]
