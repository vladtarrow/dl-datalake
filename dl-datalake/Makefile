.PHONY: install test clean

install:
	pip install -r requirements.txt

test:
	pytest tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

init:
	python -m dl_datalake.cli init

lint:
	ruff check .

lint-fix:
	ruff check . --fix

format:
	black .

type-check:
	pyright

ci: lint format-check type-check test

format-check:
	black . --check
