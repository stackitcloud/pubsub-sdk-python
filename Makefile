LICENSE_IGNORE := -ignore '.venv/**' -ignore '**/__pycache__/**' -ignore '.pytest_cache/**' -ignore '.ruff_cache/**' -ignore 'dist/**' -ignore 'htmlcov/**'

install:
	uv sync --group dev
	uv pip install -e .

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

typecheck:
	uv run ty check

check: lint format typecheck
	uv run ruff format --check src/ tests/

test: check install
	uv run pytest

test-cov: check install
	uv run pytest --cov=src/pubsub_sdk_python --cov-branch --cov-report=term-missing --cov-fail-under=85

license-add:
	addlicense -c "Schwarz Digits Cloud GmbH & Co. KG" -l apache $(LICENSE_IGNORE) .

license-check:
	addlicense -check -c "Schwarz Digits Cloud GmbH & Co. KG" -l apache $(LICENSE_IGNORE) .
