"""Integration tests for the STACKIT PubSub Python SDK."""

from __future__ import annotations

import uuid

import pytest

from pubsub_sdk_python.models import PublishMessage


@pytest.mark.integration
async def test_publish_and_purge(publisher):
    """Test publishing messages and purging the topic."""
    messages = [PublishMessage.from_plain(f"integration-test-message-{i}") for i in range(3)]

    await publisher.publish(messages)
    assert publisher.messages_sent == 3

    await publisher.purge_topic()
    assert publisher.purge_calls >= 1


@pytest.mark.integration
async def test_publish_empty_raises(publisher):
    """Test that publishing an empty list raises an error."""
    from pubsub_sdk_python.errors import PubSubValidationError

    with pytest.raises(PubSubValidationError, match="Cannot publish an empty list"):
        await publisher.publish([])


@pytest.mark.integration
async def test_publish_batch(publisher):
    """Test publishing messages in batches."""
    messages = [PublishMessage.from_plain(f"batch-test-message-{i}") for i in range(5)]

    await publisher.publish_batch(messages, batch_size=2)
    assert publisher.messages_sent == 5


@pytest.mark.integration
async def test_publish_batch_empty(publisher):
    """Test that publishing an empty batch returns None."""
    result = await publisher.publish_batch([], batch_size=2)
    assert result is None
    assert publisher.messages_sent == 0


@pytest.mark.integration
async def test_purge_topic_with_confirm(publisher):
    """Test purging topic with confirmation."""
    await publisher.purge_topic_with_confirm("purge")
    assert publisher.purge_calls >= 1


@pytest.mark.integration
async def test_purge_topic_with_confirm_invalid(publisher):
    """Test that purging with invalid confirmation raises."""
    from pubsub_sdk_python.errors import PubSubValidationError

    with pytest.raises(PubSubValidationError, match="must be 'purge'"):
        await publisher.purge_topic_with_confirm("wrong")


@pytest.mark.integration
async def test_context_manager(publisher):
    """Test that the publisher works as a context manager."""
    from pubsub_sdk_python.publisher import PubSubPublisher

    messages = [PublishMessage.from_plain("context-manager-test")]

    async with publisher as pub:
        assert isinstance(pub, PubSubPublisher)
        await pub.publish(messages)


@pytest.mark.integration
async def test_publish_and_subscribe(subscriber, publisher):
    """Test publishing a message and subscribing to it."""
    # Publish a message
    messages = [PublishMessage.from_plain("subscribe-test-message")]
    await publisher.publish(messages)

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
    from pubsub_sdk_python.subscriber import PubSubSubscriber

    async with subscriber as sub:
        assert isinstance(sub, PubSubSubscriber)


@pytest.mark.integration
async def test_message_content_round_trip(subscriber, publisher):
    """Test that a message's content survives the publish/pull round-trip intact."""
    marker = str(uuid.uuid4())[:8]
    text = f"round-trip-test-{marker}"
    await publisher.publish([PublishMessage.from_plain(text)])

    pulled_messages = await subscriber.pull(max_messages=1)
    assert len(pulled_messages) >= 1

    msg = pulled_messages[0]
    assert msg.text == text
    assert msg.payload == text.encode("utf-8")

    await subscriber.ack([msg.ack_id])


@pytest.mark.integration
async def test_purge_removes_messages(subscriber, publisher):
    """Test that purging the topic actually removes previously published messages."""
    marker = str(uuid.uuid4())[:8]
    purged_texts = {f"purge-test-{marker}-{i}" for i in range(2)}
    messages = [PublishMessage.from_plain(text) for text in purged_texts]
    await publisher.publish(messages)

    purge_calls_before = publisher.purge_calls
    await publisher.purge_topic()
    assert publisher.purge_calls == purge_calls_before + 1
    assert publisher.messages_purged == -1

    pulled_messages = await subscriber.pull(max_messages=32)
    remaining = {msg.text for msg in pulled_messages if msg.text in purged_texts}
    assert remaining == set()


@pytest.mark.integration
async def test_pull_stream_invokes_callback(subscriber, publisher):
    """Test that pull_stream invokes the on_message callback for each pulled message."""
    marker = str(uuid.uuid4())[:8]
    text = f"pull-stream-test-{marker}"
    await publisher.publish([PublishMessage.from_plain(text)])

    received = []
    result = await subscriber.pull_stream(max_messages=1, on_message=received.append)

    assert any(msg.text == text for msg in received)
    assert any(msg.text == text for msg in result)

    ack_ids = [msg.ack_id for msg in result if msg.text == text]
    await subscriber.ack(ack_ids)


@pytest.mark.integration
async def test_pull_with_custom_parameters(subscriber, publisher):
    """Test that pull accepts non-default max_messages and long_pull_duration_ms."""
    await publisher.publish([PublishMessage.from_plain("custom-pull-params-test")])

    pulled_messages = await subscriber.pull(max_messages=1, long_pull_duration_ms=500)
    assert len(pulled_messages) <= 1

    # Upper bound for max_messages should be accepted without raising.
    upper_bound_messages = await subscriber.pull(max_messages=32, long_pull_duration_ms=500)
    assert isinstance(upper_bound_messages, list)

    if pulled_messages:
        await subscriber.ack([msg.ack_id for msg in pulled_messages])
    if upper_bound_messages:
        await subscriber.ack([msg.ack_id for msg in upper_bound_messages])


@pytest.mark.integration
async def test_pull_from_nonexistent_subscription_raises_error(pubsub_client):
    """Test that pulling from a nonexistent subscription raises PubSubAuthenticationError.

    The API checks access before existence, so an unrecognized subscription ID
    returns 403 Forbidden rather than 404 Not Found.
    """
    from pubsub_sdk_python.errors import PubSubAuthenticationError

    bogus_subscriber = pubsub_client.get_subscriber(subscription_id=str(uuid.uuid4()))
    try:
        with pytest.raises(PubSubAuthenticationError):
            await bogus_subscriber.pull(max_messages=1)
    finally:
        await bogus_subscriber.close()


@pytest.mark.integration
async def test_ack_nack_messages_empty_list_is_noop(subscriber):
    """Test that ack_messages/nack_messages are no-ops for an empty message list."""
    acked_before = subscriber.messages_acked
    nacked_before = subscriber.messages_nacked

    result_ack = await subscriber.ack_messages([])
    result_nack = await subscriber.nack_messages([])

    assert result_ack is None
    assert result_nack is None
    assert subscriber.messages_acked == acked_before
    assert subscriber.messages_nacked == nacked_before


@pytest.mark.integration
async def test_publish_calls_counter(publisher):
    """Test that publish_calls increments once per publish() call."""
    calls_before = publisher.publish_calls

    await publisher.publish([PublishMessage.from_plain("counter-test-1")])
    await publisher.publish([PublishMessage.from_plain("counter-test-2")])

    assert publisher.publish_calls == calls_before + 2
