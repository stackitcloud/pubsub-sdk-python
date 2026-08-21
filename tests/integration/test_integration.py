"""Integration tests for the STACKIT PubSub Python SDK."""

from __future__ import annotations

import pytest

from pubsub_python_sdk.models import PublishMessage


@pytest.mark.integration
async def test_publish_and_purge(publisher):
    """Test publishing messages and purging the topic."""
    messages = [PublishMessage.from_plain(f"integration-test-message-{i}") for i in range(3)]

    response = await publisher.publish(messages)
    assert response.message_ids is not None
    assert len(response.message_ids) == 3
    assert publisher.messages_sent == 3

    await publisher.purge_topic()
    assert publisher.purge_calls >= 1


@pytest.mark.integration
async def test_publish_empty_raises(publisher):
    """Test that publishing an empty list raises an error."""
    from pubsub_python_sdk.errors import PubSubValidationError

    with pytest.raises(PubSubValidationError, match="Cannot publish an empty list"):
        await publisher.publish([])


@pytest.mark.integration
async def test_publish_batch(publisher):
    """Test publishing messages in batches."""
    messages = [PublishMessage.from_plain(f"batch-test-message-{i}") for i in range(5)]

    responses = await publisher.publish_batch(messages, batch_size=2)
    assert len(responses) > 0
    assert publisher.messages_sent == 5


@pytest.mark.integration
async def test_publish_batch_empty(publisher):
    """Test that publishing an empty batch returns empty list."""
    responses = await publisher.publish_batch([], batch_size=2)
    assert responses == []
    assert publisher.messages_sent == 0


@pytest.mark.integration
async def test_purge_topic_with_confirm(publisher):
    """Test purging topic with confirmation."""
    await publisher.purge_topic_with_confirm("purge")
    assert publisher.purge_calls >= 1


@pytest.mark.integration
async def test_purge_topic_with_confirm_invalid(publisher):
    """Test that purging with invalid confirmation raises."""
    from pubsub_python_sdk.errors import PubSubValidationError

    with pytest.raises(PubSubValidationError, match="must be 'purge'"):
        await publisher.purge_topic_with_confirm("wrong")


@pytest.mark.integration
async def test_context_manager(publisher):
    """Test that the publisher works as a context manager."""
    from pubsub_python_sdk.publisher import PubSubPublisher

    messages = [PublishMessage.from_plain("context-manager-test")]

    async with publisher as pub:
        assert isinstance(pub, PubSubPublisher)
        response = await pub.publish(messages)
        assert response.message_ids is not None


@pytest.mark.integration
async def test_publish_and_subscribe(subscriber, publisher):
    """Test publishing a message and subscribing to it."""
    # Publish a message
    messages = [PublishMessage.from_plain("subscribe-test-message")]
    response = await publisher.publish(messages)
    assert response.message_ids is not None

    # Pull the message from subscription
    pulled_messages = await subscriber.pull(max_messages=1)
    assert len(pulled_messages) >= 1
    assert subscriber.messages_received >= 1


@pytest.mark.integration
async def test_ack_message(subscriber, publisher):
    """Test acknowledging a message after pulling it."""
    # Publish a message
    messages = [PublishMessage.from_plain("ack-test-message")]
    await publisher.publish(messages)

    # Pull the message
    pulled_messages = await subscriber.pull(max_messages=1)
    assert len(pulled_messages) >= 1

    # Ack the message
    ack_ids = [msg.ack_id for msg in pulled_messages]
    await subscriber.ack(ack_ids)
    assert subscriber.messages_acked >= 1


@pytest.mark.integration
async def test_nack_message(subscriber, publisher):
    """Test negatively acknowledging a message after pulling it."""
    # Publish a message
    messages = [PublishMessage.from_plain("nack-test-message")]
    await publisher.publish(messages)

    # Pull the message
    pulled_messages = await subscriber.pull(max_messages=1)
    assert len(pulled_messages) >= 1

    # Nack the message
    nack_ids = [msg.ack_id for msg in pulled_messages]
    await subscriber.nack(nack_ids)
    assert subscriber.messages_nacked >= 1


@pytest.mark.integration
async def test_ack_messages_helper(subscriber, publisher):
    """Test the ack_messages helper method."""
    messages = [PublishMessage.from_plain("ack-helpers-test")]
    await publisher.publish(messages)

    pulled = await subscriber.pull(max_messages=1)
    await subscriber.ack_messages(pulled)
    assert subscriber.messages_acked >= 1


@pytest.mark.integration
async def test_nack_messages_helper(subscriber, publisher):
    """Test the nack_messages helper method."""
    messages = [PublishMessage.from_plain("nack-helpers-test")]
    await publisher.publish(messages)

    pulled = await subscriber.pull(max_messages=1)
    await subscriber.nack_messages(pulled)
    assert subscriber.messages_nacked >= 1


@pytest.mark.integration
async def test_context_manager_subscriber(subscriber):
    """Test that the subscriber works as a context manager."""
    from pubsub_python_sdk.subscriber import PubSubSubscriber

    async with subscriber as sub:
        assert isinstance(sub, PubSubSubscriber)
