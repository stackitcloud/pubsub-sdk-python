# pubsub-sdk-python

Python SDK for the **STACKIT PubSub Data Plane API** — publish messages to topics and pull/ack/nack them from subscriptions.

This SDK covers the **data plane only** (publish, pull, ack, nack). Creating/deleting topics and subscriptions, and granting publisher/subscriber access, is done via the separate STACKIT PubSub **control plane** REST API (see `tests/integration/conftest.py` for example calls) or the STACKIT Portal — not by this package.

## Install

Not published to PyPI yet — every GitHub Release publishes `pubsub-sdk-python` to PyPI automatically (see `.github/workflows/release.yml`), so once a release is cut:

```bash
pip install pubsub-sdk-python
# or: uv add pubsub-sdk-python
```

Until then, install from source with [uv](https://docs.astral.sh/uv/):

```bash
uv sync --group dev
uv pip install -e .
```

Requires Python >= 3.11. Runtime dependencies: `httpx`, `stackit-authorization`.

## Auth

Authentication goes through `AuthConfig` + `StackitAuthProvider`, backed by a STACKIT service account key.

```python
from pubsub_sdk_python import AuthConfig, PubSubClient

auth_config = AuthConfig(
    service_account_key=open("sa-key.json").read(),  # or the raw JSON string
    token_endpoint="https://authorization.stackit.cloud/auth/token",  # default shown
)
```

`AuthConfig` fields: `service_account_key`, `token_endpoint` (default `https://authorization.stackit.cloud/auth/token`), `client_id`, `client_secret`, `scope` (default `"dataplane"`). Tokens are fetched via `stackit-authorization` and cached until ~60s before expiry.

## Quickstart

```python
import asyncio
from pubsub_sdk_python import PubSubClient, PublishMessage

async def main():
    client = PubSubClient(topic_id="<topic-id>", auth_config=auth_config)

    async with client.get_publisher() as publisher:
        response = await publisher.publish([PublishMessage.from_plain("hello")])
        print(response.message_ids)

    async with client.get_subscriber(subscription_id="<subscription-id>") as subscriber:
        messages = await subscriber.pull(max_messages=10)
        for msg in messages:
            print(msg.text)  # base64-decoded payload
        await subscriber.ack([msg.ack_id for msg in messages])
        # or: await subscriber.ack_messages(messages)

asyncio.run(main())
```

`client.base_url` defaults to `https://{topic_id}.pubsub.eu01.onstackit.cloud`; pass `base_url=` to `PubSubClient(...)` to override (e.g. for other regions/environments).

`PublishMessage.from_plain(str)` / `.from_bytes(bytes)` base64-encode the payload. `Message.payload` (bytes) / `Message.text` (str) base64-decode it back.

Both `PubSubPublisher` and `PubSubSubscriber` support `async with`, and expose call counters: `publisher.messages_sent` / `.publish_calls` / `.purge_calls` / `.messages_purged`, `subscriber.messages_received` / `.messages_acked` / `.messages_nacked` / `.pull_calls` / `.ack_calls` / `.nack_calls`.

Other methods: `publisher.publish_batch(messages, batch_size=100)`, `publisher.purge_topic()` / `.purge_topic_with_confirm("purge")`, `subscriber.pull_stream(on_message=callback)`, `subscriber.nack(...)` / `.nack_messages(...)`.

## Errors

All exceptions subclass `PubSubError`:

| Exception | Raised when |
|---|---|
| `PubSubValidationError` | Bad request (HTTP 400/413), or local validation (empty message list, >100 messages, `max_messages`/`long_pull_duration_ms` out of range, empty ack/nack list) |
| `PubSubNotFoundError` | HTTP 404 |
| `PubSubAuthenticationError` | HTTP 401, or token acquisition failure |
| `PubSubServerError` | HTTP 5xx (transient — triggers a retry, see below) |
| `PubSubRetryError` | All retry attempts exhausted; `.causes` holds the underlying exceptions |
| `PubSubTimeoutError` | Request timed out (also a `TimeoutError`) |

## Retries

`PubSubPublisher`/`PubSubSubscriber` retry automatically on network/timeout errors (`httpx.TimeoutException`, `httpx.NetworkError`, `OSError`, `ConnectionError`, `TimeoutError`) and HTTP 500/502/503/504, with exponential backoff + jitter. Configure via `retry_config=RetryConfig(...)` passed to the publisher/subscriber constructor (or `get_publisher()`/`get_subscriber()` kwargs):

```python
from pubsub_sdk_python import RetryConfig

retry_config = RetryConfig(max_retries=3, base_delay=0.5, max_delay=30.0, multiplier=2.0)
```

400/401/404/413 are never retried. The standalone decorators `with_retry`/`with_async_retry` (in `pubsub_sdk_python.retry`) apply the same policy to arbitrary functions.

## Testing

```bash
uv run pytest                          # unit tests (mocked HTTP), 85% coverage gate
uv run pytest tests/integration/ --no-cov   # integration tests, hits a real STACKIT environment
```

Integration tests require `SA_KEY_B64` (or `SA_KEY`/`SA_KEY_PATH`), `PROJECT_ID`, `TOKEN_URL`, `REGION` in the environment — they create real topics/subscriptions via the control plane and clean up after themselves.

## License

No `LICENSE` file is present in this repository yet.
