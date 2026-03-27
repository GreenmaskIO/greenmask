# Integration Tests

This directory contains Python-based integration tests for Greenmask using `pytest` and `allure`.

## Prerequisites

- Python 3.11+
- Poetry (for dependency management)
- Docker & Docker Compose (for running Greenmask services)

## Setup

1. Install dependencies:
   ```bash
   poetry install
   ```

2. (Optional) Run Greenmask environment:
   Ensure your `greenmask` service or target environment is running.
   Export the service URL if different from default:
   ```bash
   export GREENMASK_URL=http://localhost:8080
   ```

## Running Tests

Run tests with Allure (report data generated in `allure-results`):
```bash
poetry run pytest --alluredir=./allure-results
```

To view the Allure report:
```bash
poetry run allure serve ./allure-results
```

## Structure

- `tests/`: Contains test files.
- `tests/conftest.py`: Fixtures (async client, configuration).
- `pyproject.toml`: Dependency and tool configuration.
