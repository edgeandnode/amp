# Python Nozzle Client

Client for issuing queries to a nozzle server and working with the returned data.

## Installation
 
1. Ensure you have [`uv`](https://docs.astral.sh/uv/getting-started/installation/) installed locally.
2. Install dependencies
    ```bash
    uv build 
   ```
3. Activate a virtual environment 
    ```bash
    uv venv
   ```

## Useage 

### Marimo

Start up a marimo workspace editor
```bash
uv run marimo edit
```

The Marimo app will open a new browser tab where you can create a new notebook, view helpful resources, and 
browse existing notebooks in the workspace.

### Apps

You can execute python apps and scripts using `uv run <path>` which will give them access to the dependencies 
and the `nozzle` package. For example, you can run the `execute_query` app with the following command.
```bash
uv run apps/execute_query.py
```

# Self-hosted nozzle server

In order to operate a local nozzle server you will need to have the files 
that [`dump`](../dump) produces available locally, and run the [server](../server) 
You can then use it in your python scripts, apps or notebooks.

# Testing

The project is set up to use the [`pytest`](https://docs.pytest.org/en/stable/) testing framework. 
It follows [standard python test discovery rules](https://docs.pytest.org/en/stable/explanation/goodpractices.html#test-discovery). 

Run all tests
```bash
uv run pytest
```

# Linting and formatting

Ruff is configured to be used for linting and formatting of this project. 

Run formatter
```bash
uv run ruff format
```

Run linter 
```bash
uv run ruff check .
```

Run linter and apply auto-fixes
```bash 
uv run ruff check . --fix
```
