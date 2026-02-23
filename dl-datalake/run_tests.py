"""Run tests for dl-datalake."""

import sys

import pytest

if __name__ == "__main__":
    print(f"Running from {sys.executable}")  # noqa: T201
    sys.exit(pytest.main(["tests/integration", "-v"]))
