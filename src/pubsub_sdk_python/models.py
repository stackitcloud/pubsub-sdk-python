"""Data models generated from the OpenAPI specification for STACKIT PubSub Data Plane API."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime


@dataclass
class PublishMessage:
    """A single message to publish to a topic.

    Attributes:
        data: Base64-encoded message payload.
    """

    data: str

    @staticmethod
    def from_plain(text: str) -> PublishMessage:
        """Create a PublishMessage from plain text (auto base64-encodes)."""
        encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
        return PublishMessage(data=encoded)

    @staticmethod
    def from_bytes(raw: bytes) -> PublishMessage:
        """Create a PublishMessage from raw bytes (auto base64-encodes)."""
        encoded = base64.b64encode(raw).decode("ascii")
        return PublishMessage(data=encoded)


@dataclass
class PublishMessageToTopicRequest:
    """Request body for publishing messages to a topic."""

    messages: list[PublishMessage]


@dataclass
class PublishMessageResponse:
    """Response from a successful publish operation."""

    message_ids: list[int]


@dataclass
class Message:
    """A message returned from a pull operation."""

    id: int
    ack_id: str
    data: str
    create_time: datetime
    delivery_attempts: int = 1

    @property
    def payload(self) -> bytes:
        """Decode the base64-encoded data payload."""
        return base64.b64decode(self.data)

    @property
    def text(self) -> str:
        """Decode the data as UTF-8 text."""
        return self.payload.decode("utf-8")


@dataclass
class Messages:
    """Container for a list of messages returned from pull."""

    messages: list[Message]


@dataclass
class AckMessagesFromTopicRequest:
    """Request body for acknowledging messages."""

    ack_ids: list[str]


@dataclass
class NackMessagesFromTopicRequest:
    """Request body for negatively acknowledging messages."""

    nack_ids: list[str]


def decode_message_data(data: str) -> bytes:
    """Decode a base64-encoded message data field."""
    return base64.b64decode(data)


def decode_message_text(data: str) -> str:
    """Decode a base64-encoded message data field as UTF-8 text."""
    return base64.b64decode(data).decode("utf-8")


__all__ = [
    "AckMessagesFromTopicRequest",
    "Message",
    "Messages",
    "NackMessagesFromTopicRequest",
    "PublishMessage",
    "PublishMessageResponse",
    "PublishMessageToTopicRequest",
    "decode_message_data",
    "decode_message_text",
]
