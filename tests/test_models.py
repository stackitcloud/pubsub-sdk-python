"""Tests for data models."""

import base64
from datetime import datetime

from pubsub_sdk_python.models import (
    AckMessagesFromTopicRequest,
    Message,
    Messages,
    NackMessagesFromTopicRequest,
    PublishMessage,
    PublishMessageResponse,
    PublishMessageToTopicRequest,
    decode_message_data,
    decode_message_text,
)


class TestPublishMessage:
    """Test PublishMessage model."""

    def test_from_plain_text(self):
        msg = PublishMessage.from_plain("Hello")
        expected = base64.b64encode(b"Hello").decode("ascii")
        assert msg.data == expected

    def test_from_bytes(self):
        msg = PublishMessage.from_bytes(b"\x00\xff\xfe")
        expected = base64.b64encode(b"\x00\xff\xfe").decode("ascii")
        assert msg.data == expected

    def test_data_is_base64_encoded(self):
        msg = PublishMessage.from_plain("test message")
        decoded = base64.b64decode(msg.data)
        assert decoded == b"test message"


class TestPublishMessageToTopicRequest:
    """Test PublishMessageToTopicRequest model."""

    def test_creates_request(self):
        messages = [PublishMessage.from_plain("Hello"), PublishMessage.from_plain("World")]
        request = PublishMessageToTopicRequest(messages=messages)
        assert request.messages == messages
        assert len(request.messages) == 2


class TestPublishMessageResponse:
    """Test PublishMessageResponse model."""

    def test_has_message_ids(self):
        response = PublishMessageResponse(message_ids=[1, 2, 3])
        assert response.message_ids == [1, 2, 3]

    def test_empty_message_ids(self):
        response = PublishMessageResponse(message_ids=[])
        assert response.message_ids == []


class TestMessage:
    """Test Message model."""

    def test_create_message(self):
        msg = Message(
            id=1,
            ack_id="ack-1",
            data=base64.b64encode(b"Hello").decode("ascii"),
            create_time=datetime(2024, 1, 1, 0, 0, 0),
            delivery_attempts=1,
        )
        assert msg.id == 1
        assert msg.ack_id == "ack-1"
        assert msg.delivery_attempts == 1

    def test_payload_decodes_base64(self):
        msg = Message(
            id=1,
            ack_id="ack-1",
            data=base64.b64encode(b"test data").decode("ascii"),
            create_time=datetime(2024, 1, 1, 0, 0, 0),
        )
        assert msg.payload == b"test data"

    def test_text_decodes_utf8(self):
        msg = Message(
            id=1,
            ack_id="ack-1",
            data=base64.b64encode(b"Hello, World!").decode("ascii"),
            create_time=datetime(2024, 1, 1, 0, 0, 0),
        )
        assert msg.text == "Hello, World!"


class TestMessages:
    """Test Messages container."""

    def test_container(self):
        msg = Message(
            id=1,
            ack_id="ack-1",
            data=base64.b64encode(b"test").decode("ascii"),
            create_time=datetime(2024, 1, 1, 0, 0, 0),
        )
        container = Messages(messages=[msg])
        assert len(container.messages) == 1
        assert container.messages[0].id == 1


class TestAckNackRequests:
    """Test ack/nack request models."""

    def test_ack_request(self):
        request = AckMessagesFromTopicRequest(ack_ids=["ack-1", "ack-2"])
        assert request.ack_ids == ["ack-1", "ack-2"]

    def test_nack_request(self):
        request = NackMessagesFromTopicRequest(nack_ids=["nack-1", "nack-2"])
        assert request.nack_ids == ["nack-1", "nack-2"]


class TestDecodeHelpers:
    """Test module-level decode helpers."""

    def test_decode_message_data(self):
        encoded = base64.b64encode(b"test").decode("ascii")
        result = decode_message_data(encoded)
        assert result == b"test"

    def test_decode_message_text(self):
        encoded = base64.b64encode(b"test text").decode("ascii")
        result = decode_message_text(encoded)
        assert result == "test text"
