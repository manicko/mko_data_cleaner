# 05 — Dictionary Path Override via CLI Flag

- **Status:** ✅ Research complete. Approach B (factory parameter) selected with confirmed PO decisions. Specification ready for implementation.
- **Date:** 2026-10-28
- **PO decisions confirmed:** 2026-10-28 (Q1=(b), Q2=(a), Q3=(a), Q4=(b))
- **Source request:** `.ai/problems/Prob_01` (Russian):
  > Сейчас у нас папка отчета предполагает наличие папки со словарем — `dict`.
  > Нужно сделать альтернативный вариант, когда путь к файлу словаря можно
  > будет задать отдельно, через флаг в консоли.
  > Мы говорим про единый конечный файл словаря на основе которого идёт очистка.
  > У нас ещё есть вариант, когда единого файла нет и его собираем из файлов в
  > папке. Его не рассматриваем.
- **Evidence base:** repository inspection (`app.py`, `app_service.py`, `csv_service.py`,
  `models.py`, `paths.py`), the Researcher's architecture trace (Approach B selected),
  runtime behavior documented in `current-behavior.md` and `architecture.md`.

---

## 1. Problem Statement

The report directory layout **requires** the dictionary to live at a fixed,
config-driven location inside the report folder:

```
<report_dir>/
├── raw_data/
├── dict/merged_dictionary.csv   ← hardcoded via data_paths.dict_file = "dict/merged_dictionary.csv"
├── clean_data/
```

The path is computed by `AppService.dict_path`
(`app_service.py:74-76`):

```python
@property
def dict_path(self) -> Path:
    return Path(self.base_path, self.app_config.data_paths.dict_file)
```

This ties the dictionary to the report directory. The user wants to be able to
**point to a dictionary file anywhere on disk** via a console flag — a single
consolidated `merged_dictionary.csv` — without restructuring the report folder.

The folder-merge fallback (`CSVWorker.get_merged_dictionary()`,
`csv_service.py:208-231`) — where the tool scans `dict/` for fragment files and
merges them — is **explicitly out of scope** per the request ("Его не рассматриваем").

---

## 2. Confirmed Requirements

| # | Requirement | Basis |
|---|---|---|
| R1 | A console flag allows specifying an alternative single-file dictionary path | Prob_01: "путь к файлу словаря можно будет задать отдельно, через флаг в консоли" |
| R2 | The flag applies to a *single consolidated dictionary file* (not folder-merge) | Prob_01: "единый конечный файл словаря на основе которого идёт очистка" |
| R3 | Folder-merge of dictionary fragments is NOT in scope | Prob_01: "Его не рассматриваем" |
| R4 | When no flag is given, behavior is identical to today (backward compatible) | Implied by "alternative option" |
| R5 | The Python API `process_data()` mirrors the CLI flag (dual entry points) | Project design: CLI and `process_data` are equivalent entry points, both already accept `settings_dir` |
| R6 | When the flag IS given and the file does not exist, the tool fails with a clear error showing the resolved path searched — no silent fallback to folder-merge | PO decision Q2 |
| R7 | Relative paths in `--dict-path` resolve against the report directory (not CWD); absolute paths are accepted as-is | PO decision Q1 |

---

## 3. Conceptual Development Tasks

| # | Purpose | Expected Outcome | Dependencies |
|---|---|---|---|
| T1 | Add `dict_path` parameter to `AppService.__init__` and `get_app_service()`; modify the `dict_path` property to resolve the override against `base_path` | Override stored at construction-time; property resolves relative paths against `base_path` (report dir) and keeps absolute paths as-is | Researcher: Approach B confirmed feasible |
| T2 | Add `--dict-path` Typer Option to the CLI `run` command in `app.py` | `mko-data-cleaner run <report_dir> [--dict-path PATH]` accepts the flag; `exists=False` (validated in `run_report`, not Typer) | T1 (threading) |
| T3 | Add `dict_path` parameter to `process_data()` public API; thread to `get_app_service` | `process_data(report_path, settings_dir=None, dict_path=None)` mirrors the CLI | T1 |
| T4 | Add existence validation in `run_report` after `base_path` is set | When `dict_path` override is given and the resolved file doesn't exist, a `FileNotFoundError` is raised with a detailed message showing the resolved path searched | T1 (property updated) |
| T5 | Update the mock in `test_app.py` to accept the new `dict_path` parameter | `test_process_data_returns_report_result` mock lambda updated; `assert_called_once_with` adjusted | T1, T3 |
| T6 | Add tests for the override behavior | Tests: (a) override path is used, (b) default behavior unchanged, (c) missing override file raises clear error with resolved path in message, (d) CLI `--dict-path` end-to-end, (e) relative path resolves against report dir | T2–T4 |
| T7 | Update documentation | README (`--dict-path` flag + `dict_path` param on `process_data`), `current-behavior.md`, `architecture.md` | T2, T3 |

---

## 4. Product Owner Decisions (Confirmed 2026-10-28)

### Q1 — Path resolution semantics → **(b)** Relative to report directory

When a user runs `mko-data-cleaner run /path/to/report --dict-path my_dict.csv`,
how should the relative path be resolved?

| # | Option | Verdict |
|---|---|---|
| (a) | Relative to current working directory (matches `--settings-dir`) | **Rejected** |
| **(b)** | **Relative to report directory** — `my_dict.csv` → `/path/to/report/my_dict.csv` | **Confirmed** |

**PO statement:** "Но, готовы принимать полный путь в первую очередь." (Absolute
paths are accepted as-is and are the primary use case; relative paths resolve
against the report directory.)

**Rationale for (b):** The dictionary is an input specific to the report
directory, just like `raw_data/`. Resolving relative to the report dir makes the
command portable and intuitive. Absolute paths (the typical usage) are unaffected
by this decision.

**Implementation consequence:** `self._dict_path` is stored as-is; the `dict_path`
property resolves it via `Path(self.base_path, self._dict_path)`. Python's `pathlib`
automatically returns the absolute path if `_dict_path` is already absolute
(`Path("/report", "/abs/path.csv")` → `/abs/path.csv`), and joins relative paths
with `base_path` (`Path("/report", "my_dict.csv")` → `/report/my_dict.csv`).

**Typer consequence:** `exists=False` is used on the Typer option (not `exists=True`).
Typer's `exists=True` would validate against CWD at CLI parsing time, conflicting
with report-dir-relative resolution for relative paths. Validation is deferred to
`run_report` where `base_path` is available.

### Q2 — Missing file behavior → **(a)** Clear error with detailed path

If `--dict-path /some/path/dict.csv` is provided but the file does not exist:

| # | Option | Verdict |
|---|---|---|
| **(a)** | **Raise a clear, immediate error** showing the resolved path searched | **Confirmed** |
| (b) | Silently fall back to `<report_dir>/dict/merged_dictionary.csv` | **Rejected** |

**PO statement:** "важно при отсутствии файла показывать пользователю подробную
ошибку с указанием конкретного пути — где мы искали файл и где его не нашли."
(It's important to show the user a detailed error indicating the specific path
where we looked for the file and where we didn't find it.)

**Implementation consequence:** In `run_report`, after `self.base_path` is set
(line 117), before `CSVWorker` construction (line 126), add:

```python
if self._dict_path is not None and not self.dict_path.is_file():
    raise FileNotFoundError(
        f"Dictionary file not found.\n"
        f"  Override path: {self._dict_path}\n"
        f"  Resolved path: {self.dict_path}\n"
        f"  Searched at: {self.dict_path.resolve()}\n"
        f"Remove --dict-path / dict_path parameter to use the default "
        f"(<report_dir>/{self.app_config.data_paths.dict_file})."
    )
```

The error is caught by `app.py`'s `except Exception as e:` handler (line 87),
which prints `❌ Ошибка: {e}` and exits with code 1.

### Q3 — Python API parity → **(a)** Yes

Should `process_data()` accept a `dict_path` parameter?

| # | Option | Verdict |
|---|---|---|
| **(a)** | **Yes** — `process_data(report_path, settings_dir=None, dict_path=None)` | **Confirmed** |
| (b) | No — CLI-only | Rejected |

### Q4 — ReportResult field → **(b)** No

Should the `ReportResult` include the effective dictionary file path?

| # | Option | Verdict |
|---|---|---|
| (a) | Yes — add `dict_path: Path` to `ReportResult` | Rejected |
| **(b)** | **No** — keep `ReportResult` as-is (dict path is input, not output) | **Confirmed** |

---

## 5. Research Summary

### 5.1 Data Flow (as-is)

```
CLI: app.py run(report, verbose, settings_dir)
     → get_app_service(settings_dir)         # factory + settings gate
     → service.run_report(report)

Python API: app.py process_data(report_path, settings_dir)
            → get_app_service(settings_dir).run_report(path)

Factory: app_service.py get_app_service(settings_dir)
         → AppService(app_paths=..., resolver=...)  # __init__ sets _base_path=None
         → logging.dictConfig(log_config)

Orchestration: app_service.py run_report(data_path)
   line 117: self.base_path = data_path          # property setter: resolve + ensure_dir
   line 118: self.resolver.ensure_file_parent(self.db_path)
   line 119: self.resolver.ensure_dir(self.export_path)
   line 126: csv_worker = CSVWorker(...,
   line 130:     dict_path=self.dict_path,       # ← THE INJECTION POINT (property)
   ...
   line 137: df = csv_worker.get_dictionary()

Property: app_service.py dict_path (line 74-76)
   return Path(self.base_path, self.app_config.data_paths.dict_file)
   # e.g. <report_dir>/dict/merged_dictionary.csv

CSVWorker: csv_service.py
   __init__ (line 45): self.dict_path = Path(dict_path)
   get_dictionary (line 203-206):
     if not self.dict_path.is_file():
         self.get_merged_dictionary()    # folder-merge fallback (OUT OF SCOPE)
     return pl.read_csv(self.dict_path, **self.reader_settings)
```

**Key finding:** `CSVWorker` already accepts `dict_path` as a constructor parameter
and already checks `is_file()` before falling through to the merge path. A
single-file override pointing to an existing file requires **zero changes to
`csv_service.py`** — the existing code path handles it correctly.

### 5.2 Implementation Approaches Evaluated

| | A: Thread through `run_report` | **B: Thread through `get_app_service` factory** | C: Inline in `run_report` |
|---|---|---|---|
| Mechanism | `run_report(data_path, dict_path=None)`; property checks `self._dict_path` | `get_app_service(settings_dir, dict_path=None)`; `__init__` stores `self._dict_path`; property checks it | `run_report(data_path, dict_path=None)`; inline `dict_path or self.dict_path` passed to CSVWorker |
| Property modified? | Yes (3 lines) | Yes (3 lines) | No |
| `__init__` modified? | Yes (1 line) | Yes (2 lines) | No |
| `run_report` signature change? | Yes | No | Yes |
| `get_app_service` signature change? | No | Yes | No |
| Matches `--settings-dir` pattern? | No (different layer) | **Yes** (factory-level, identical to `settings_dir`) | No (different layer) |
| Test impact | `assert_called_once_with` needs `dict_path=None` | Mock lambda needs `dict_path=None` | `assert_called_once_with` needs `dict_path=None` |
| Est. LOC changed | ~14 | ~16 | ~11 |
| Single source of truth | Property | **Property** | Inline expression |

### 5.3 Recommended Approach: B (Factory Parameter)

**Rationale:**

1. **Matches the established `--settings-dir` pattern.** The `--settings-dir` flag
   is defined identically in all three CLI commands and threaded exclusively
   through `get_app_service(settings_dir)`. Adding `dict_path` as a sibling
   parameter at the same layer is the most natural, consistent extension.

2. **Preserves `run_report`'s public contract.** `run_report(data_path)` is the
   documented orchestration method (`architecture.md` sequence diagram:
   `A->>S: run_report(report_dir)`). Keeping its signature unchanged means the
   architecture diagram stays accurate and no downstream consumer of `run_report`
   is affected.

3. **Property remains the single source of truth.** The `dict_path` property is
   accessed in one place (`run_report` line 130 → `CSVWorker`). With the override
   stored in `__init__` and checked at the top of the property, all callers
   transparently get the correct path. The property resolves relative paths
   against `base_path` (report dir) via `Path(self.base_path, self._dict_path)`,
   satisfying PO decision Q1=(b).

### 5.4 Typer Best Practices (from Researcher + PO decisions)

The recommended Typer option definition, matching the `--settings-dir` style
(`app.py:61-69`) but with `exists=False` (per Q1=(b), validation is deferred):

```python
dict_path: Annotated[
    Path | None,
    typer.Option(
        exists=False,          # NOT exists=True — Typer validates against CWD,
                               # but we resolve relative to the report dir (Q1)
        file_okay=True,
        dir_okay=False,        # must be a file, not a directory
        help="Override the dictionary file path (default: <report_dir>/dict/merged_dictionary.csv). "
             "Relative paths resolve against the report directory.",
    ),
] = None,
```

- `exists=False`: consistent with `--settings-dir` (which also uses `exists=False`).
  File existence is validated later in `run_report` when `base_path` is available,
  allowing report-dir-relative resolution (Q1=(b)).
- `dir_okay=False`: a directory is never a valid single dictionary file.
- **Existence check (in `run_report`):** raises `FileNotFoundError` with the
  resolved path shown in the message (Q2=(a), PO requirement for detailed error).

---

## 6. Proposed Changes (Approach B)

### 6.1 Source code changes

| File | Line(s) | Change |
|------|---------|--------|
| `app.py` | ~60 (in `run` signature) | Add `dict_path: Annotated[Path \| None, typer.Option(exists=False, file_okay=True, dir_okay=False, help="…")] = None` |
| `app.py` | 77 | `service = get_app_service(settings_dir, dict_path=dict_path)` |
| `app.py` | 126 | Add `dict_path: Path \| None = None` to `process_data` |
| `app.py` | 145 | `return get_app_service(settings_dir, dict_path=dict_path).run_report(path)` |
| `app_service.py` | 36 | `def __init__(self, app_paths: AppPaths, resolver: PathResolver, dict_path: Path \| None = None):` |
| `app_service.py` | 40 | `self._dict_path = dict_path` (alongside `self._base_path = None`) |
| `app_service.py` | 74-76 | `dict_path` property: add guard `if self._dict_path is not None: return Path(self.base_path, self._dict_path)` before existing return |
| `app_service.py` | 117-118 (in `run_report`) | After `self.base_path = data_path`, add existence validation: `if self._dict_path is not None and not self.dict_path.is_file(): raise FileNotFoundError(...)` with detailed message showing resolved path |
| `app_service.py` | 327 | `def get_app_service(settings_dir: Path \| None = None, dict_path: Path \| None = None) -> AppService:` |
| `app_service.py` | 338 | `AppService(app_paths=app_paths, resolver=PathResolver(app_paths.user_dir), dict_path=dict_path)` |
| `csv_service.py` | — | **No changes needed** |
| `models.py` | — | **No changes needed** (per Q4 decision: No) |

### 6.2 Test changes

| File | Line(s) | Change |
|------|---------|--------|
| `tests/test_app.py` | 308-311 | Update mock lambda: `lambda settings_dir=None, dict_path=None: fake_service` |
| `tests/test_app.py` | 316 | **No change** — `run_report` signature is unchanged in Approach B; assertion stays `assert_called_once_with(empty_report_dir)`. Only the mock lambda (line 310) needs the new `dict_path` kwarg. |
| `tests/test_app.py` | (new) | `test_process_data_with_dict_path_override` — external dict file (outside report dir) is used; correct rows cleaned |
| `tests/test_app.py` | (new) | `test_process_data_without_dict_path_uses_default` — default behavior unchanged (same as existing end-to-end test) |
| `tests/test_app.py` | (new) | `test_process_data_dict_path_missing_raises_filenotfound` — non-existent override `dict_path` raises `FileNotFoundError`; message contains resolved path and "Dictionary file not found" |
| `tests/test_app.py` | (new) | `test_run_cli_with_dict_path` — CLI `--dict-path` end-to-end via `CliRunner`; override file used correctly |
| `tests/test_app.py` | (new) | `test_dict_path_relative_resolves_against_report_dir` — relative `dict_path` resolves against the report directory (Q1) |
| `tests/conftest.py` | (new fixture) | Add `external_dict_file` fixture: a `merged_dictionary.csv` placed in `tmp_path` outside the report directory |

### 6.3 Documentation changes

| File | Change |
|------|--------|
| `README.md` | Document `--dict-path` CLI flag and `dict_path` param on `process_data`; explain relative-vs-absolute resolution and the "file not found" error |
| `docs/00-overview/current-behavior.md` | Add `--dict-path` to the CLI entry point table (line 32) and to the report directory layout section (line 69-81) as an alternative to the default `dict/` location |
| `docs/00-overview/architecture.md` | Update the System Context sequence diagram (line 194-220) to show `dict_path` threading through `get_app_service` → `AppService.__init__` → `dict_path` property |

---

## 7. Assumptions

| # | Assumption | Basis | Risk if wrong |
|---|---|---|---|
| A1 | The dictionary is always a single consolidated CSV file when the override is used | Prob_01: "единый конечный файл словаря" | If users need folder-merge with an override, the feature doesn't support it |
| A2 | The `dict_path` property is the only consumer of the dictionary path in `run_report`; no other code reads `data_paths.dict_file` directly | Code-verified: `dict_path` property (app_service.py:76) is the sole consumer in `run_report`; `WorkingPaths.dict_file` is only accessed there | If another component reads `dict_file` directly, the override would not propagate |
| A3 | `CSVWorker.get_dictionary()` behavior is correct for an override file that exists: reads it directly, no merge | Code-verified: `get_dictionary` (csv_service.py:203-206) checks `is_file()` → reads directly; the override always points to an existing file (enforced by the explicit existence check in `run_report`) | None |
| A4 | Absolute paths are the primary use case for `--dict-path`; relative paths are a convenience | PO: "готовы принимать полный путь в первую очередь" | If users heavily rely on relative paths, the report-dir resolution (Q1=(b)) must be clearly documented |
| A5 | `Path(base_path, override)` correctly handles both absolute and relative paths | Python `pathlib` behavior: absolute second argument overrides base; relative joins | If path handling changes in future Python, the property may need revisiting — unlikely |

---

## 8. Constraints

- **No `csv_service.py` changes** — the existing `CSVWorker.__init__` / `get_dictionary()` already handles a single-file path correctly.
- **No `models.py` changes** (per Q4 decision) — `WorkingPaths` and `ReportResult` are untouched.
- **Backward compatible** — when `dict_path` is `None`, `self._dict_path` stays `None` and the property falls through to `Path(self.base_path, self.app_config.data_paths.dict_file)` — identical to current behavior.
- **No folder-merge support** — the override bypasses the merge path entirely; the file must exist (validated in `run_report`).
- **`exists=False` on Typer** — per Q1=(b), validation is deferred to `run_report` where `base_path` is available for report-dir-relative resolution. This is consistent with `--settings-dir` which also uses `exists=False`.
- **`run_report` signature unchanged** — per Approach B, the override is injected at construction time via `get_app_service`, preserving the `run_report(data_path)` contract.

---

## 9. Risks

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | **Folder-merge fallback on non-existent override file** (HIGH) | If `--dict-path` points to a missing file and no early check is performed, `get_dictionary()` (csv_service.py:204-205) falls through to `get_merged_dictionary()` which scans the *override's parent directory* — not the report's `dict/` folder. This silently merges wrong files. | Existence check in `run_report` (line 117-118, after `base_path` is set, before CSVWorker at line 126). `is_file()` on the resolved `dict_path` catches both absolute and report-dir-relative paths. |
| R2 | **Typer `exists=True` would break Q1=(b)** (MEDIUM) | If `exists=True` were used on the Typer option, Typer validates the path against CWD at parse time. A relative path like `my_dict.csv` would be checked as `CWD/my_dict.csv`, but the PO wants it resolved as `report_dir/my_dict.csv`. Users passing relative paths would get false "not found" errors. | Use `exists=False` (matching `--settings-dir`). Perform existence validation in `run_report` where `base_path` is available. |
| R3 | **Test mock needs updating** (LOW) | `test_process_data_returns_report_result` (test_app.py:308-316) uses a mock lambda `lambda settings_dir=None: fake_service` and asserts `assert_called_once_with(empty_report_dir)`. Adding `dict_path` to `get_app_service` breaks both. | Update lambda to `lambda settings_dir=None, dict_path=None: fake_service` and assertion to `assert_called_once_with(empty_report_dir, dict_path=None)`. |
| R4 | **Python API existence check** (LOW) | `process_data(report, dict_path="relative.csv")` resolves relative to `report_dir` (via property), but users might expect CWD resolution (like `--settings-dir`). The detailed error message must show the resolved path so users can debug. | Error message includes "Override path", "Resolved path", and "Searched at" (absolute resolved form). PO explicitly requested: "показывать пользователю подробную ошибку с указанием конкретного пути — где мы искали файл и где его не нашли." |
| R5 | **Temporal ordering of `_dict_path` vs `_base_path`** (NONE) | `_dict_path` is set at construction (via `get_app_service`); `_base_path` is set inside `run_report` (line 117). The existence check runs after `base_path` is set; the property `Path(self.base_path, self._dict_path)` works only after `base_path` is set. | The check is placed at line 117-118 (after `self.base_path = data_path`), before CSVWorker construction (line 126). Verified by code reading — no action needed. |

---

## 10. Open Questions

All product-level questions were answered by the Product Owner on 2026-10-28.
No open questions remain for this specification.

---

## 11. Out of Scope

- **Folder-merge fallback** — `CSVWorker.get_merged_dictionary()` is not triggered
  when an override is given (the file must exist). Merging dictionary fragments
  from a folder remains the domain of the default `dict_file` config only.
  (Prob_01: "Его не рассматриваем".)
- **GUI / web interface** — no UI changes.
- **Dictionary format changes** — the CSV structure, `col_indexes`, and
  `WorkingPaths` config model are untouched.
- **`ReportResult` extension** — per Q4 decision, no new field is added to the
  result object.
- **Settings template changes** — `app_config.json5` is not modified; the
  override is a runtime flag, not a config option.
- **Dictionary fragment merging with override** — only a single pre-merged
  file is supported via the flag.
- **CWD-relative path resolution** — per Q1 decision, relative paths resolve
  against the report directory, not CWD. This differs from `--settings-dir`
  (which resolves CWD-relative). This intentional inconsistency is documented.
- **i18n of user-facing messages** — the current error handling pattern in
  `app.py` (line 88: `❌ Ошибка: {e}`) prints Russian prefix; the error body
  from `run_report` will be in English. Full i18n is tracked separately (OQ-10
  in `open-questions.md`).

---

## 12. Definition of Ready

All items below are **satisfied** by this specification:

1. ✅ **PO decisions confirmed:** Q1=(b) (report-dir-relative), Q2=(a) (error with detailed path), Q3=(a) (API parity), Q4=(b) (no ReportResult changes). Confirmed by Product Owner on 2026-10-28.
2. ✅ **Implementation approach selected:** Approach B (factory parameter) — `get_app_service(settings_dir, dict_path=None)` → `AppService.__init__` stores `_dict_path` → `dict_path` property resolves override against `base_path`.
3. ✅ **Acceptance criteria defined:**
   - `mko-data-cleaner run <report> --dict-path /abs/path/dict.csv` uses that file.
   - `mko-data-cleaner run <report> --dict-path my_dict.csv` resolves to `<report>/my_dict.csv` (Q1=(b)).
   - Without `--dict-path`, behavior is byte-for-byte identical to today.
   - A non-existent `--dict-path` file produces a `FileNotFoundError` showing the resolved path searched (Q2).
   - `process_data(report, dict_path="/abs/path/dict.csv")` works identically to the CLI.
   - All existing tests pass with the one mock lambda update.
4. ✅ **Test plan defined:** New tests cover override success, default fallback, missing-file error with path in message, CLI `--dict-path` end-to-end, and relative-path report-dir resolution.
5. ✅ **Doc update plan acknowledged:** README, `current-behavior.md`, and `architecture.md` updated.
6. ✅ **Risks identified and mitigated:** R1 (folder-merge fallback) mitigated by early check in `run_report`; R2 (Typer `exists=True` conflict) mitigated by `exists=False`.
