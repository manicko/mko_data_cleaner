# Project Commands — mko_data_cleaner

**Env:** Windows 11 · `uv` (Python 3.12+) · no Docker · no external services

This is a **standalone uv project**. All commands run locally via `uv run`. There is
no Docker Compose, no PostgreSQL, no Django, and no Makefile.

## Bootstrap

```powershell
# One-time: install uv (if not already installed)
winget install astral-sh.uv

# Install/sync dependencies (dev group included by default)
uv sync --group dev
```

## Quick reference

| Task | Command |
|---|---|
| Lint | `uv run ruff check src/ tests/` |
| Auto-fix (incl. import sorting) | `uv run ruff check --fix src/ tests/` |
| Format | `uv run ruff format src/ tests/` |
| Typecheck | `uv run basedpyright src/` |
| Run tests | `uv run pytest` |
| Run a single test | `uv run pytest tests/test_app.py::test_name -v` |
| Run a single test file | `uv run pytest tests/test_app.py -v` |
| Add dependency | `uv add <pkg>` |
| Add dev dependency | `uv add --dev <pkg>` |
| Run package CLI | `uv run mko-data-cleaner` |

## Lint & Format

`ruff check --fix` handles import sorting (I001) and auto-fixes. `ruff format` only
reformats (line wraps, quotes) and does **NOT** sort imports — use
`ruff check --fix` for sorting.

Lint scope is `src/ tests/` (the `scripts/` directory is excluded; see `pyproject.toml`).

## Tests

`uv run pytest` runs the full suite from `pyproject.toml` config:

```toml
[tool.pytest.ini_options]
addopts = ["--import-mode=importlib", "-ra", "-v", "--strict-markers"]
pythonpath = ["src"]
testpaths = ["tests"]
```

**Running specific tests:**

```powershell
# Single test by name
uv run pytest -k test_name

# Single file
uv run pytest tests/test_app.py

# Single function
uv run pytest tests/test_app.py::test_process_data_end_to_end
```

**Test gate:** `uv run pytest` → all tests must pass.

**Test fixtures** — source of truth: `tests/conftest.py`

Canonical fixtures: `db_worker` (SQLite in tmp_path), `empty_report_dir`,
`populated_settings`, `synthetic_report`, `sample_dictionary`, `dict_indexes`.
Use `mko_data_cleaner.core.errors.WrongDataSettings` and other error types as needed.

## Typecheck

```powershell
uv run basedpyright src/
```

`basedpyright` config is in `pyproject.toml` (`[tool.basedpyright]`, `typeCheckingMode = "standard"`).
