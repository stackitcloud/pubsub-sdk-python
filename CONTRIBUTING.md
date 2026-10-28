# Contributing to STACKIT PubSub Python SDK

Thank you for your interest in contributing to the STACKIT PubSub Python SDK! We welcome contributions, bug reports, and suggestions from the community.

Please review this guide before getting started. All contributors are expected to uphold our [Code of Conduct](CODE_OF_CONDUCT.md). If you encounter any issues regarding conduct, please contact `pubsub@stackit.com`.

---

## Reporting Issues & Proposing Features

We use GitHub Issues to track bugs and discuss feature proposals.

### Reporting Bugs

When filing a bug report, please provide as much context as possible to help us diagnose and resolve the issue quickly:

- **Descriptive Title:** Summarize the issue succinctly (e.g., `PubSubSubscriber.pull hangs indefinitely when the long-pull duration elapses`).
- **Description:** Clearly state what happened versus what you expected to happen.
- **Minimal Reproducible Example:** Provide a minimal Python snippet demonstrating the problem (client initialization, calls made, exception thrown).
- **Environment Details:**
  - Python version (e.g., `python --version`)
  - SDK version (git commit or tag)
  - Operating system and architecture
- **Logs & Error Output:** Include relevant log messages or stack traces. **Important:** Always scrub sensitive information (service account keys, authorization tokens, project secrets) before posting.

### Proposing Features

Before writing code for major changes or new features, please open an issue to discuss the design with the maintainers:

- **Problem & Use Case:** Describe the problem you are solving and why the SDK should support this feature.
- **Proposed Solution:** Outline the suggested API design or method signatures (e.g., how the caller would use it).
- **Alternatives & Compatibility:** Mention any alternative approaches considered, and note if the proposal involves breaking changes.

---

## Pull Request Guidelines

### Workflow & Commits

1. **Branch Naming:** Create a focused branch off `main` with a descriptive prefix:
   - `feat/add-batch-publish-support`
   - `fix/subscriber-ack-race`
   - `docs/clarify-error-handling`
2. **Commit Style:** Use concise commit messages, ideally following [Conventional Commits](https://www.conventionalcommits.org/) (e.g., `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`).
3. **Scope:** Keep pull requests small and focused on a single concern or bug fix. Avoid bundling unrelated refactorings or dependency changes.

### PR Description Checklist

When opening a PR, include:

- **Summary / Motivation:** Explain why the change is needed and what it accomplishes.
- **Changes Made:** A concise bulleted list of modifications.
- **Linked Issues:** Reference relevant issues (e.g., `Fixes #123` or `Closes #45`).
- **Testing Performed:** Note the tests added or executed to verify the change.
- **Breaking Changes:** Explicitly state if this change breaks backward compatibility and why.

### Review Process

Once your PR is created, maintainers are requested for review. Please address review feedback directly in your branch. Once approved and all CI checks pass, a maintainer will merge the PR.

---

## Testing Requirements

We maintain a high bar for reliability and test coverage:

- **Tests are Mandatory:** Every new feature and bug fix must be accompanied by tests. PRs without tests will not be merged unless the change is purely documentation or tooling.
- **Framework:** The project uses [pytest](https://docs.pytest.org/) with [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) for testing. Tests are located in `tests/`.
- **Coverage & Edge Cases:**
  - Verify happy paths as well as error conditions (e.g., `PubSubError` subtypes, retry exhaustion).
  - Branch coverage must stay at or above 85% (enforced by `make test-cov`).
- **Running Tests Locally:**
  - Run the test suite:
    ```bash
    make test
    ```
  - Integration tests interact with STACKIT PubSub APIs and expect configuration variables (`SA_KEY_B64`, `STACKIT_PROJECT_ID`). Run them with `uv run pytest tests/integration/ --no-cov`.

---

## Development Workflow & CI Verification

### Local Commands

| Command              | Description                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| `make install`       | Installs the dev dependencies and the SDK in editable mode via `uv`.        |
| `make check`         | Runs `ruff` lint and format checks.                                         |
| `make test`          | Runs `make check`, followed by the unit test suite.                         |
| `make test-cov`      | Like `make test`, but enforces the 85% branch coverage gate.                |
| `make license-add`   | Adds missing Apache 2.0 license headers to source files using `addlicense`. |
| `make license-check` | Verifies that all source files carry the license header.                    |

### What Runs in CI

Every push and pull request triggers our GitHub Actions pipelines:

1. **`check-license`** (`.github/workflows/ci.yml`): verifies license headers with `addlicense`.
2. **`lint`** and **`test`** (`.github/workflows/ci.yml`): run `ruff` and the unit tests with the coverage gate.
3. **`integration`** (`.github/workflows/integration.yml`): runs the integration tests against the live STACKIT PubSub API.

Before submitting your PR, ensure that `make license-check` and `make test-cov` pass locally.
