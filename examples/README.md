# Examples

Self-contained, executable scripts demonstrating the `pubsub-sdk-python` SDK.
Each script is runnable on its own (`python examples/<script>.py`) and only
depends on the SDK's existing runtime dependencies (`httpx`,
`stackit-authorization`) - no extra packages to install.

| Script                    | Demonstrates                                                             |
|---------------------------|---------------------------------------------------------------------------|
| `01_publish_message.py`   | Client instantiation, auth from a service account key, publishing.      |
| `02_pull_and_ack.py`      | Pulling messages from a subscription, then ack/nack.                    |
| `03_error_handling.py`    | Catching each `PubSubError` subclass, including retry exhaustion.       |

## Prerequisites

Install the SDK (from the repo root):

```bash
uv sync --group dev
uv pip install -e .
```

## Environment variables

| Variable                             | Required by         | Description                                                              |
|---------------------------------------|----------------------|----------------------------------------------------------------------------|
| `STACKIT_SERVICE_ACCOUNT_KEY_PATH`   | 01, 02, 03 (part 2)  | Filesystem path to a STACKIT service account key JSON file.             |
| `STACKIT_TOPIC_ID`                   | 01, 02, 03 (part 2)  | ID of an existing PubSub topic.                                          |
| `STACKIT_SUBSCRIPTION_ID`            | 02                   | ID of an existing subscription on that topic.                            |
| `STACKIT_TOKEN_ENDPOINT` (optional)  | all                  | OAuth2 token endpoint override.                                          |
| `STACKIT_BASE_URL` (optional)        | all                  | Dataplane base URL override.                                             |

None of these scripts embed credentials or hardcode a key path - the
service account key is always read from a file whose location comes from
`STACKIT_SERVICE_ACCOUNT_KEY_PATH`, keeping secrets out of source control.

Obtain (or rotate) a service account key using the STACKIT Portal or the
STACKIT CLI (`stackit service-account key create ...`); the CLI is only
used externally to produce the key file, it is not called from these
scripts.

If a required variable is missing, each script prints a clear message and
exits with a non-zero status instead of raising an unhandled traceback.

## Running

```bash
export STACKIT_SERVICE_ACCOUNT_KEY_PATH=/path/to/sa-key.json
export STACKIT_TOPIC_ID=<topic-id>
export STACKIT_SUBSCRIPTION_ID=<subscription-id>

python examples/01_publish_message.py
python examples/02_pull_and_ack.py
python examples/03_error_handling.py
```

`03_error_handling.py` also runs its first part (local validation-error
handling) with no environment variables set at all, since that part never
makes a network call.
