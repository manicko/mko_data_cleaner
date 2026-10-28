---
phase: "02"
phase_name: "config"
id_prefix: "CFG"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/02-audit-config.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Configuration Management

## Summary

7 findings, highest severity **HIGH**. The settings gate raises on missing files
(CRITICAL band is empty — no regression), but a malformed-but-present config
file passes the gate and is surfaced to the operator as an opaque pydantic
`ValidationError` rather than a cause-specific, file-naming error: the
JSON5 loader masks parse failures by returning `{}`, `app_config` is validated
lazily (so service construction can even succeed), and the intended
`ConfigError` wrap is dead code because it catches the never-raised
`DataValidationError`. Three MEDIUM findings cover config-adjacent dead/unconsumed
code surface (an unused helper and three declared-but-unimported dependencies).
Three LOW findings cover entry/operational posture gaps the phase explicitly
scopes (missing `python -m` support, non-atomic template copy, stale dev
`__main__` blocks) plus doc-code divergence where the overview docs still
describe the pre-YAML→JSON5 state the code has already corrected. Packaging
metadata staleness (rich, `__version__`, top-level re-exports) is noted as
related but deferred to the packaging phase (12).

## Scope

**Examined:** `src/mko_data_cleaner/app.py`, `core/app_service.py`
(`get_app_service`, `_require_settings`, `build_app_paths`,
`AppService.app_config`/`log_config`), `core/init_service.py`
(`_copy_templates`, `init_project`), `core/utils.py` (`config_to_dict`,
`merge_dicts`), `core/paths.py`, `core/models.py`, `settings/app_config.json5`,
`settings/log_config.json5`, `pyproject.toml` `[project].dependencies`,
`README.md`, `docs/00-overview/current-behavior.md` and `open-questions.md`,
`tests/test_app.py`, `tests/conftest.py`.

**Tools:** `rg`/`grep`, `ruff check src/ tests/` (clean), `basedpyright src/`
(39 type-narrowing diagnostics — not config-scope), `pytest` (93 passed),
`uv run python` runtime repro.

**Not verified:** end-to-end run over the real 43 MB sample; gzip and FTS5
rule paths (per current-behavior.md limitations).

<!-- GATES — run before appending; each falsifies a fabricated finding:
     1. every cited path appears in `git ls-files`
     2. the symbol exists in that file, and the disambiguator token exists inside it
     3. no `:<digits>` appears in any locator field
     4. every "we ran X" claim has its output pasted in the fence below it -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| CFG-001 | HIGH | `src/mko_data_cleaner/core/utils.py::config_to_dict` | Malformed config passes the gate and surfaces as an opaque ValidationError; the ConfigError wrap is dead code |
| CFG-002 | MEDIUM | `pyproject.toml::[project].dependencies` | Three declared direct dependencies are never imported by project source |
| CFG-003 | MEDIUM | `src/mko_data_cleaner/core/utils.py::merge_dicts` | Dead helper `merge_dicts` has zero call sites |
| CFG-004 | LOW | `src/mko_data_cleaner/app.py::__main__` | `python -m mko_data_cleaner` fails — no package `__main__.py` |
| CFG-005 | LOW | `src/mko_data_cleaner/core/init_service.py::_copy_templates` | Non-atomic template copy on `--force` (no temp+rename) |
| CFG-006 | LOW | `src/mko_data_cleaner/core/app_service.py::__main__` | Stale dev-only `__main__` blocks hardcode paths/behavior in importable modules |
| CFG-007 | LOW | `docs/00-overview/current-behavior.md::"settings_dir is accepted but not forwarded"` | Overview docs describe a pre-migration config state the code has since fixed |

#### CFG-001 — [HIGH] Malformed config passes the gate and surfaces as an opaque ValidationError; the ConfigError wrap is dead code

**Anchor:** `src/mko_data_cleaner/core/utils.py::config_to_dict`; `src/mko_data_cleaner/core/app_service.py::_require_settings`; `src/mko_data_cleaner/core/app_service.py::AppService.app_config`; `src/mko_data_cleaner/core/app_service.py::AppService.log_config`
**Disambiguator:** `return {}`
**Checked:** ran
**Why this band:** phase HIGH = "Malformed-but-present config passes the gate and surfaces as an opaque downstream error"; here a malformed file passes the existence-only gate and the user receives a `pydantic_core ValidationError` naming missing *fields* (not the malformed file), and the documented `ConfigError` never wraps it. Adjacent MEDIUM (dead code / drift) is not used because this is a malformed config reaching an opaque downstream failure, not merely unused code.
**Problem:** The settings gate `_require_settings` (app_service.py:316-319) checks only `.exists()` on `app_config.json5`/`log_config.json5`, never their validity. `config_to_dict` (utils.py:144-146) catches `ValueError` and returns `{}` — so a syntactically broken JSON5 file is silently turned into an empty dict and its parse error is demoted to a `logger.error` line. `app_config` is a lazily-evaluated `cached_property` read only inside `run_report`, so `get_app_service()` constructs the service **without error** even when `app_config.json5` is malformed; the failure is deferred to the first `app_config` access in `run_report` (app_service.py:68), where `DataSettings(**{})` raises `pydantic.ValidationError`. The intended `except DataValidationError as e: raise ConfigError(...)` (app_service.py:49-52) can never fire, because `DataValidationError` (errors.py:12) is never raised anywhere and is not a subclass of pydantic's `ValidationError`. The documented "wrapped in `ConfigError`" (current-behavior.md:286) never happens. A malformed `log_config.json5` is evaluated eagerly (via `prepare_log_paths` in `__init__`) and surfaces the same opaque `ValidationError` at construction.
**Repro:** (1) temp user dir with valid `log_config.json5` + malformed `app_config.json5` (`key: value: extra`); (2) `get_app_service(user_dir)` returns an `AppService` with no error; (3) `process_data(report, settings_dir=user_dir)` raises `pydantic_core._pydantic_core.ValidationError`; (4) the only hint that the file was malformed is the `utils` log line `Unexpected "k" at column 1` — the user-facing error says `6 validation errors for DataSettings … Field required`.
**Impact:** An operator who ships a broken settings file (e.g. a stray trailing comma, or the legacy YAML `key: value: extra` shape the docs historically used) is misdirected into "fix your config fields" when the real cause is a syntax error in the settings file; the error names no file and offers no remediation, and for `app_config` the failure is deferred to run rather than fail-fast at startup.
**Root cause:** (1) existence-only gate, (2) `config_to_dict` swallows parse errors and returns a blank dict, (3) `app_config` is lazily validated so it is not caught at service construction, (4) the `ConfigError` wrap targets the wrong exception type (`DataValidationError` is never raised by pydantic), making the handler dead code.
**Fix:** Make `config_to_dict` raise (or re-raise) the parse error instead of returning `{}`; eagerly validate both models in the factory (access `app_config` in `get_app_service`/construction); and wrap `pydantic.ValidationError` in a `ConfigError`/settings error that names the offending file. Targets: `utils.py::config_to_dict`, `app_service.py::_require_settings`, `app_service.py::AppService.app_config`.
**Verify:** new test needed: `test_malformed_app_config_raises_named_config_error` (asserts a `ConfigError`/named exception pointing at `app_config.json5`, not a raw `pydantic.ValidationError`, and that it raises at `get_app_service`). **Remediation blockers (assert the current masking behavior):** `tests/test_app.py::test_config_to_dict_returns_empty_dict_on_invalid_json5` (test_app.py:250) and `tests/test_app.py::test_config_to_dict_returns_empty_dict_on_missing_file` (test_app.py:245); docs `current-behavior.md:286` asserts the (false) ConfigError wrap.
**Related:** CFG-005, CFG-007

**Evidence — `rg -n` of the gate, the lazy property, and the dead handler:**
```text
src/mko_data_cleaner/core/utils.py:144:    except (FileNotFoundError, ValueError) as err:
src/mko_data_cleaner/core/utils.py:146:        return {}
src/mko_data_cleaner/core/app_service.py:316:    if not app_paths.app_config.exists():
src/mko_data_cleaner/core/app_service.py:46:    @cached_property
src/mko_data_cleaner/core/app_service.py:47:    def app_config(self) -> DataSettings:
src/mko_data_cleaner/core/app_service.py:51:        except DataValidationError as e:
src/mko_data_cleaner/core/errors.py:12: class DataValidationError(Exception):
```
*(supports: config_to_dict returns `{}` on parse error; gate is existence-only; app_config is a lazy cached_property; the except targets DataValidationError, which is never raised — confirmed: `rg -n "raise DataValidationError" src tests` → no matches; `issubclass(DataValidationError, pydantic.ValidationError)` → False.)*

**Evidence — runtime repro (.ai/tmp/repro_consolidated.py):**
```text
--- malformed app_config.json5 + valid log_config.json5 ---
CONSTRUCTED OK (gate passed; app_config is lazily validated): AppService
--- both settings files missing ---
SettingsNotInitializedError (gate works): Missing file(s): app_config.json5, log_config.json5.
--- process_data with malformed app_config.json5 ---
19-10-26 03:02:54 PM - ERROR - <string>:1 Unexpected "k" at column 1 - mko_data_cleaner.core.utils
USER-FACING ERROR TYPE: pydantic_core._pydantic_core.ValidationError
is ConfigError? False
first line: 6 validation errors for DataSettings
```
*(supports: malformed config passes the gate and constructs; missing config raises the gate; the user-facing error is a raw `ValidationError`, not `ConfigError`.)*

#### CFG-002 — [MEDIUM] Three declared direct dependencies are never imported by project source

**Anchor:** `pyproject.toml::[project].dependencies`
**Disambiguator:** `pydantic-settings`
**Checked:** static
**Why this band:** phase MEDIUM = "declared dependencies never imported" and "Dead or ineffective & unconsumed configuration"; `pydantic-settings`, `python-dateutil`, `urllib3` are declared direct deps with zero imports in `src/` or `tests/`. Adjacent HIGH (opaque config corruption) is not used — these are unused declared deps, not a data-correctness hazard.
**Problem:** `pyproject.toml` declares `pydantic-settings>=2.12.0` (line 25), `python-dateutil>=2.8.0` (line 26), and `urllib3>=2.1.0` (line 32) as direct dependencies, but none is imported by the project's own source. Config is loaded with `json5` + plain `pydantic.BaseModel`, not `pydantic-settings`; the tool is local-only and performs no HTTP, so `urllib3`/`dateutil` are not exercised. `rg -n "pydantic_settings|urllib3|dateutil" src tests` → no matches.
**Repro:** static — after `import mko_data_cleaner.app`, `sys.modules` contains none of `pydantic_settings`/`urllib3`, and only `dateutil` (loaded transitively, e.g. by polars — not by project code).
**Impact:** Each unused direct dependency enlarges install/CI resolution surface, pins versions the code does not use, and increases the security-revision burden; `pydantic-settings` in particular implies a half-finished settings approach (Pydantic `BaseSettings`) that was abandoned in favor of manual json5 loading.
**Root cause:** Dependencies were declared (likely for a planned approach) but the implementation never imported them; declarations were never pruned.
**Fix:** Remove `pydantic-settings`, `urllib3`, and `python-dateutil` from `[project].dependencies` (or mark them justified/optional) only after confirming no transitive requirement forces a specific version. Target: `pyproject.toml::[project].dependencies`.
**Verify:** after removal, `uv run pytest` (93) green and `uv run mko-data-cleaner --help` works; add `test_no_unused_direct_dependencies` (parse `[project].dependencies` and assert each name is imported in `src/`).
**Related:** CFG-003

**Evidence — `rg` for the three names in src+tests, and the pyproject declarations:**
```text
$ rg -n "pydantic_settings|urllib3|dateutil" src tests
(no matches)
$ rg -n "pydantic-settings|python-dateutil|urllib3" pyproject.toml
pyproject.toml:25:    "pydantic-settings>=2.12.0",
pyproject.toml:26:    "python-dateutil>=2.8.0",
pyproject.toml:32:    "urllib3>=2.1.0",
```
*(supports: none of the three is imported by project source despite being declared.)*

#### CFG-003 — [MEDIUM] Dead helper `merge_dicts` has zero call sites

**Anchor:** `src/mko_data_cleaner/core/utils.py::merge_dicts`
**Disambiguator:** `def merge_dicts`
**Checked:** static
**Why this band:** phase MEDIUM = "Dead or ineffective & unconsumed configuration (e.g., a helper with zero call sites)"; `merge_dicts` is defined and self-recursive but is never called. Adjacent LOW (stale code) is not used because this is explicitly flagged by phase 05 as a Phase-02 concern.
**Problem:** `utils.merge_dicts` (utils.py:149) implements a recursive dict overlay merge, but this tool deliberately has **no** config overlay merge (phase 05: "No network, no API, no merge overlay (`merge_dicts` dead — Phase 02)"). It has zero call sites in `src/` or `tests/` (the only references are its own definition and its recursive call, plus the phase text).
**Repro:** static — `rg -n "merge_dicts" src tests` → only `utils.py:149` (def) and `utils.py:167` (recursive call).
**Impact:** Dead, untested code that suggests a config-merge feature exists; it can rot unnoticed and confuse readers about the loading model (single-file JSON5 load, no overlay).
**Root cause:** Leftover from a prior config-merging design; never removed.
**Fix:** Delete `merge_dicts` (and its `Any`-typed signature) from `utils.py`, or add a real caller. Target: `src/mko_data_cleaner/core/utils.py::merge_dicts`.
**Verify:** `rg -n "merge_dicts" src tests` → no matches; `uv run pytest` green.
**Related:** CFG-002

#### CFG-004 — [LOW] `python -m mko_data_cleaner` fails — no package `__main__.py`

**Anchor:** `src/mko_data_cleaner/app.py::__main__`
**Disambiguator:** `app()`
**Checked:** ran
**Why this band:** phase LOW = "missing `__main__.py` so the bare package entry fails"; the bare package entry `python -m mko_data_cleaner` does not launch the CLI. Adjacent MEDIUM (entry-surface parity leak that changes effective config) is not used — config resolution is consistent; only the invocation form is missing.
**Problem:** The package has no `src/mko_data_cleaner/__main__.py`. `python -m mko_data_cleaner` fails; the CLI is reachable only via the submodule form `python -m mko_data_cleaner.app` (app.py:153) or the `mko-data-cleaner` console script.
**Repro:** `uv run python -m mko_data_cleaner` → `No module named mko_data_cleaner.__main__; 'mko_data_cleaner' is a package and cannot be directly executed`.
**Impact:** Breaks the conventional `python -m <package>` entry (phase block 6 asks whether the bare package entry launches the CLI — it does not); minor portability gap for direct module invocation.
**Root cause:** No `__main__.py` was added; only `app.py` carries the `if __name__ == "__main__"` guard.
**Fix:** Add `src/mko_data_cleaner/__main__.py` with `from mko_data_cleaner.app import app; app()`, and include it in package sources. Target: new file `src/mko_data_cleaner/__main__.py`.
**Verify:** `uv run python -m mko_data_cleaner --help` → exit 0; add `test_python_minus_m_package_launches_cli` (subprocess `python -m mko_data_cleaner --help`).
**Related:** CFG-006

**Evidence — runtime:**
```text
$ uv run python -m mko_data_cleaner
No module named mko_data_cleaner.__main__; 'mko_data_cleaner' is a package and cannot be directly executed
```
*(supports: the bare package entry fails; only `python -m mko_data_cleaner.app` is supported.)*

#### CFG-005 — [LOW] Non-atomic template copy on `--force`

**Anchor:** `src/mko_data_cleaner/core/init_service.py::_copy_templates`
**Disambiguator:** `shutil.copy2`
**Checked:** static
**Why this band:** phase LOW = "non-atomic template copy"; `_copy_templates` writes templates with `shutil.copy2` directly to the destination. Adjacent MEDIUM (data corruption on normal path) is not used because corruption only occurs during an interrupted `--force` re-init, not on normal use.
**Problem:** On `init --force`, `_copy_templates` writes each template with `shutil.copy2` straight into the user settings directory (init_service.py:27) with no write-to-temp-then-rename. A crash/interruption mid-copy can leave a truncated `app_config.json5`/`log_config.json5`, which then becomes a *malformed* config (see CFG-001) instead of failing cleanly — and the user has no backup.
**Repro:** static — read `init_service.py:26-27`: `target_path.parent.mkdir(...)` then `shutil.copy2(item, target_path)`; no `NamedTemporaryFile`/`os.replace` surround.
**Impact:** A partial write silently produces a masking malformed config (CFG-001); recovery is manual. Low on normal runs, but it turns a safe "regenerate settings" command into one that can brick the user's only settings.
**Root cause:** Template copy uses a direct `shutil.copy2` rather than an atomic-replace pattern.
**Fix:** Write each file to a sibling temp path then `os.replace` into place; optionally back up the previous file when `force=True`. Target: `src/mko_data_cleaner/core/init_service.py::_copy_templates`.
**Verify:** `test_init_force_atomic_writes` (monkeypatch `shutil.copy2` to raise mid-copy; assert the destination file is unchanged/intact, and a temp is cleaned up).
**Related:** CFG-001

#### CFG-006 — [LOW] Stale dev-only `__main__` blocks hardcode paths/behavior in importable modules

**Anchor:** `src/mko_data_cleaner/core/app_service.py::__main__`; `src/mko_data_cleaner/core/init_service.py::__main__`
**Disambiguator:** `r"data\mvideo"`
**Checked:** static
**Why this band:** phase LOW = "Stale dev-only execution blocks in production modules"; two core modules carry `if __name__ == "__main__"` dev quick-runs. Adjacent MEDIUM (config leak) is not used — these blocks don't change config resolution; they're just stale scaffolding.
**Problem:** `app_service.py:343-344` runs `get_app_service().run_report(r"data\mvideo")` and `init_service.py:56-57` runs `init_project(force=True)` when those modules are executed directly. These embed a hardcoded Windows path and an unconditional `force=True` clobber inside the importable `core` package; the supported entry is the console script or `python -m mko_data_cleaner.app` (app.py:153).
**Repro:** static — `rg -n '__main__' src/mko_data_cleaner/core/app_service.py src/mko_data_cleaner/core/init_service.py` → the two blocks; `rg -n 'data\\mvideo' src/mko_data_cleaner/core/app_service.py` → line 344.
**Impact:** Low direct runtime risk (guards only run on direct module execution), but misleading stale scaffolding in the core package; the `data\mvideo` path is Windows-only and `init_project(force=True)` silently overwrites user settings with no confirmation.
**Root cause:** Dev quick-runs left behind when the CLI/console-script entry became canonical.
**Fix:** Remove the `if __name__ == "__main__"` blocks from `app_service.py` and `init_service.py` (keep app.py:153 as the CLI entry). Targets: `src/mko_data_cleaner/core/app_service.py` (lines 343-344), `src/mko_data_cleaner/core/init_service.py` (lines 55-57).
**Verify:** `rg -n '__main__' src/mko_data_cleaner/core` → only `app_service.py`/`init_service.py` gone; `uv run pytest` + `uv run mko-data-cleaner --help` green.
**Related:** CFG-004

#### CFG-007 — [LOW] Overview docs describe a pre-migration config state the code has since fixed

**Anchor:** `docs/00-overview/current-behavior.md::"settings_dir is accepted but not forwarded"`; `docs/00-overview/current-behavior.md::"tests and telebot logger entries"`; `docs/00-overview/open-questions.md::"process_data returns None"`
**Disambiguator:** `"tests and telebot logger entries"`
**Checked:** static (code-verified + test-verified)
**Why this band:** phase LOW = "doc-code divergence on config/logger names" and block 5 "Does any documented config key or logger exist in docs but not in the shipped file?" and block 6 entry-surface parity. Adjacent MEDIUM (config drift that changes runtime) is not used — the code is correct here; only the docs lag. (Packaging-metadata staleness is scope-excluded to phase 12 — see Fix.)
**Problem:** Three config-relevant doc claims contradict the tested code at c0fd0e1:
- (logger names) `current-behavior.md:315-317` and `open-questions.md` D-8.6 state `log_config.json5` "still contains the unused `tests` and `telebot` logger entries" — the shipped template has only the `__main__` logger (log_config.json5:45); `tests`/`telebot` are absent.
- (config forwarding) `current-behavior.md:35` and `open-questions.md` D-6 state `initialize_settings` "does not forward settings_dir" and `process_data` "returns `None`" — the code forwards `settings_dir` (app.py:150 → init_service.py:44-50) and returns `ReportResult` (app.py:145).
- (config wrap) `current-behavior.md:286` states an invalid `app_config.json5` yields "Pydantic validation error wrapped in `ConfigError`" — the wrap never fires (CFG-001).
**Repro:** `rg -n "tests|telebot" src/mko_data_cleaner/settings/log_config.json5` → no matches; `uv run pytest tests/test_app.py::test_initialize_settings_threads_settings_dir tests/test_app.py::test_process_data_end_to_end tests/test_app.py::test_top_level_exports` → pass; `uv run python -c "from mko_data_cleaner import process_data; print('ok')"` → ok (top-level import works). The docs were dated 2026-10-15; git history shows the YAML→JSON5 refactor (`91e963b`) and the API fixes landed after, without a doc refresh.
**Impact:** Maintainers/operators following the overview docs are misled: they believe forwarding is broken, that `tests`/`telebot` loggers need cleaning, and that config errors are wrapped in `ConfigError` — all contradicting the tested behavior, causing duplicated remediation and wrong incident diagnosis.
**Root cause:** Docs were authored against a pre-YAML→JSON5 / pre-API-refactor tree and not refreshed after the refactor.
**Fix (DOC-UPDATE):** Refresh `current-behavior.md` and `open-questions.md` to match code: remove the "does not forward settings_dir / process_data returns None" claims (D-6); mark D-8.6 resolved (no `tests`/`telebot` loggers); correct the ConfigError-wrap claim (current-behavior.md:286) to reflect the opaque-`ValidationError` reality — ideally paired with the CFG-001 code fix so the wrap actually works. **Out of phase scope (deferred to phase 12):** the packaging-metadata claims in D-8 (rich "not declared", no `__version__`, "empty `__init__.py`") — rich is declared and `__version__`/re-exports exist (test-verified), so D-8.1/4 are also stale, but belong to the packaging phase.
**Verify:** after doc edits, the cited doc lines no longer contradict the cited code; `uv run pytest` still 93 passed (doc-only change).
**Related:** CFG-001, CFG-002

**Evidence — `rg` for the stale logger claims vs the shipped template:**
```text
$ rg -n "tests|telebot" src/mko_data_cleaner/settings/log_config.json5
(no matches)
$ rg -n "__main__" src/mko_data_cleaner/settings/log_config.json5
log_config.json5:45:     "__main__": { // if __name__ == "__main__"
```
*(supports: the shipped template has only the `__main__` logger, not `tests`/`telebot`.)*

**Evidence — `git ls-files` for package `__main__`:**
```text
$ git ls-files src/mko_data_cleaner/__main__.py
(empty — file does not exist)
```
*(supports: CFG-004. `app.py`, `core/app_service.py`, `core/init_service.py` are tracked; `__main__.py` is not.)*

## Fix Order

- **Dependencies:** None among these findings. CFG-001 (masking) should be fixed together with CFG-005 (atomic write) — an atomic `--force` copy prevents the truncated files that become the masking malformed configs of CFG-001.
- **Merge candidates:** CFG-002 + CFG-003 (both are dead/unconsumed surface hygiene). CFG-004 + CFG-006 (both concern `__main__` entry points).
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** CFG-007's packaging sub-claims (rich, `__version__`, empty `__init__.py`) and the 39 `basedpyright` type-narrowing diagnostics belong to the packaging (12) and typing phases, not config; not filed here.
- **Rollout risk:**
  - CFG-001: making `config_to_dict` raise (instead of `{}`) breaks `test_config_to_dict_returns_empty_dict_on_invalid_json5` and `test_config_to_dict_returns_empty_dict_on_missing_file` (test_app.py:245,250) — these assert the masking and must be rewritten as part of the fix.
  - CFG-002: removing deps must keep `uv run pytest` (93) and `uv run mko-data-cleaner --help` green.
  - CFG-006: removing the `core` `__main__` blocks leaves `app.py::__main__` as the sole CLI entry; `uv run mko-data-cleaner --help` must stay green.

## Appendices

### Appendix A — Runtime repro: malformed config passes the gate

Command: `uv run python .ai/tmp/repro_consolidated.py`
```text
--- malformed app_config.json5 + valid log_config.json5 ---
CONSTRUCTED OK (gate passed; app_config is lazily validated): AppService
--- both settings files missing ---
SettingsNotInitializedError (gate works): Missing file(s): app_config.json5, log_config.json5.
--- process_data with malformed app_config.json5 ---
19-10-26 03:02:54 PM - ERROR - <string>:1 Unexpected "k" at column 1 - mko_data_cleaner.core.utils
USER-FACING ERROR TYPE: pydantic_core._pydantic_core.ValidationError
is ConfigError? False
first line: 6 validation errors for DataSettings
```
*(supports: CFG-001 — malformed config passes the gate and constructs; missing config raises the gate; user-facing error is a raw `ValidationError`, not `ConfigError`.)*

### Appendix B — Runtime repro: bare package entry fails

Command: `uv run python -m mko_data_cleaner`
```text
No module named mko_data_cleaner.__main__; 'mko_data_cleaner' is a package and cannot be directly executed
```
*(supports: CFG-004.)*

### Appendix C — Baseline tooling

```text
$ uv run ruff check src/ tests/
All checks passed!
$ uv run pytest -q
93 passed in 12.74s
$ uv run python -c "import mko_data_cleaner; print(mko_data_cleaner.__version__)"
0.2.0
$ uv run python -c "from mko_data_cleaner import process_data; print('ok')"
ok
```
*(supports: CFG-002/CFG-004/CFG-007 — runtime facts the docs contradict; ruff clean; suite green.)*

**Block 7 (operational posture) scope note — reviewed, no defect:** there is no env-override layer for config values (only `notebook.py` reads `MKO_NO_INSTALL_CHECK`); this is acceptable for a local-only, credential-free CLI. Posture-affecting settings (`data_paths.*`, `extension`, `index_column`, `date_column`, `add_separator`, `fts_separator`, `col_indexes`, `table_name`, reader/writer options) are all template-driven and file-validated; the DB file intentionally lives inside the report directory and is deleted per run (documented in current-behavior.md:78,196,269). No finding filed.
