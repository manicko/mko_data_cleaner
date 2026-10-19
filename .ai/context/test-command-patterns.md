---
id: test-command-patterns
domain: commands
tags:
  - testing
  - uv
  - commands
related:
  - .kilo/rules/commands.md
---

# Test Command Patterns (canonical reference)

Grounded in committed configuration (`pyproject.toml`, `.github/workflows/ci.yml`).
Use these shapes so every test run is reproducible and matches CI.

| Concern | Correct (canonical) | Incorrect / fragile (anti-pattern) |
|---|---|---|
| Test command | `uv run pytest` | `pytest` directly (without `uv run` — bypasses project venv/dependencies) |
| Single test | `uv run pytest tests/test_app.py::test_name -v` | `uv run pytest -k test_name` from wrong directory (may miss imports) |
| Single file | `uv run pytest tests/test_app.py -v` | Running `pytest` without the `-v` flag (loses failure detail in CI output) |
| Lint | `uv run ruff check src/ tests/` | `ruff check` directly (without `uv run` — may use wrong ruff version) |
| Typecheck | `uv run basedpyright src/` | `basedpyright` directly (without `uv run`) |

## Equivalent direct invocation

`uv run pytest` (full suite, 65 tests) is the canonical command. CI runs the same
command with the same `pyproject.toml` config:

```toml
[tool.pytest.ini_options]
addopts = ["--import-mode=importlib", "-ra", "-v", "--strict-markers"]
pythonpath = ["src"]
testpaths = ["tests"]
```

Tests use SQLite in `tmp_path` fixtures — no external database, no Docker, no
Compose project names. Each test gets an isolated in-memory or file-backed SQLite
database via the `db_worker` fixture in `tests/conftest.py`.
