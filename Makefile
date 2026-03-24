.PHONY: install test lint format type-check pre-commit clean

install:
	uv sync --all-groups

test:
	uv run pytest tests/ -v --cov=. --cov-report=term-missing

lint:
	uv run ruff check .
	uv run black --check .
	uv run isort --check-only .

format:
	uv run black .
	uv run isort .
	uv run ruff check --fix .

type-check:
	uv run mypy .

pre-commit:
	uv run pre-commit run --all-files

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf htmlcov/ .coverage
