install:
	uv sync --group dev
	uv pip install -e .

lint:
	uvx ruff@0.16.4 check

format:
	uvx ruff@0.16.4 format

check: lint format
	uvx ruff@0.16.4 format --check

test: check install
	uv run pytest

test-cov: check install
	uv run pytest --cov=src/pubsub_python_sdk --cov-report=term-missing --cov-fail-under=85
