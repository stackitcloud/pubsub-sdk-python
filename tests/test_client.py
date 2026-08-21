"""Tests for the PubSubClient."""

from __future__ import annotations

import pytest

from pubsub_python_sdk.auth import AuthConfig
from pubsub_python_sdk.client import PubSubClient
from pubsub_python_sdk.publisher import PubSubPublisher
from pubsub_python_sdk.subscriber import PubSubSubscriber


class TestPubSubClientInit:
    """Test PubSubClient initialization."""

    def test_default_auth_config_is_created(self):
        client = PubSubClient(topic_id="my-topic")

        assert client.topic_id == "my-topic"
        assert isinstance(client.auth_config, AuthConfig)
        assert client.base_url == "https://my-topic.pubsub.eu01.onstackit.cloud"

    def test_custom_auth_config_is_used(self):
        auth_config = AuthConfig(client_id="id", client_secret="secret")

        client = PubSubClient(topic_id="my-topic", auth_config=auth_config)

        assert client.auth_config is auth_config

    def test_custom_base_url_template_is_formatted(self):
        client = PubSubClient(
            topic_id="my-topic",
            base_url="https://{topicId}.example.com",
        )

        assert client.base_url == "https://my-topic.example.com"


class TestPubSubClientFactories:
    """Test PubSubClient.get_publisher and get_subscriber."""

    def test_get_publisher_returns_configured_publisher(self):
        client = PubSubClient(topic_id="my-topic")

        publisher = client.get_publisher()

        assert isinstance(publisher, PubSubPublisher)
        assert publisher.topic_id == "my-topic"
        assert publisher.base_url == client.base_url + "/"
        assert publisher.auth_provider is client.auth_provider

    def test_get_subscriber_returns_configured_subscriber(self):
        client = PubSubClient(topic_id="my-topic")

        subscriber = client.get_subscriber(subscription_id="sub-1")

        assert isinstance(subscriber, PubSubSubscriber)
        assert subscriber.subscription_id == "sub-1"
        assert subscriber.base_url == client.base_url + "/"
        assert subscriber.auth_provider is client.auth_provider


class TestPubSubClientClose:
    """Test PubSubClient.close."""

    @pytest.mark.asyncio
    async def test_close_returns_none(self):
        client = PubSubClient(topic_id="my-topic")

        result = await client.close()

        assert result is None
