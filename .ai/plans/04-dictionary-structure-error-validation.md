# Execution Plan 04 — Informative error on dictionary structure mismatch

- **Status:** Ready for implementation
- **Created:** 2026-10-28 by Tech Lead (multi-agent workflow)
- **Source problem note:** `.ai/problems/04_uninformative_error_on_dictionary_column_mismatch.md`
- **Code context (Auditor):** `.ai/tmp/code_context.md` — plan is factually accurate; three minor
  non-blocking inaccuracies documented (wrong paths `core/app_service.py` vs `app_service.py`,
  missing `core/config.py`, misplaced line citation for `get_dictionary`).
- **Baseline HEAD:** working tree (no git repo — standalone uv project).
- **Chosen solution path:** Plan §6A (new exception) + §6B (guard `MappingDict._initialize_mapping`)
  + §6C (guard `CSVWorker.get_dictionary` — primary, has `dict_path`) + §6E (guard `build_match_query`).
  §6D (`app.py` step context) is satisfied implicitly: `DictionaryStructureError.__str__`
  embeds file path / step / issue / hint, which flows through the existing
  `console.print(f"❌ Ошибка: {e}")` catch-all — **no app.py change required** (minimal scope).

## Audit verdict (Step 0)

- `IndexError: index 1 is out of bounds for sequence of length 1` reproduced at runtime against
  `.ai/tmp/gac/` (comma-separated dict, config separator `;`, Polars 2.0.0).
- `errors.py` (53 lines): 6 exception types, none named `DictionaryStructureError`.
- `dict_service.py`:
  - `MappingDict.__init__` (line 17) has no `dict_path` parameter.
  - `_initialize_mapping` (lines 96-114): crash at line 107 `self.data[:, action_idx]`, no guard.
  - `build_match_query` (line 269): `columns_list[col_idx]` unguarded.
- `csv_service.py`: `get_dictionary()` (lines 203-206) — bare `pl.read_csv`, no validation, has `self.dict_path`.
- `app_service.py`: `run_report` (lines 138-140) constructs `MappingDict` with no try/except; `self.dict_path` (property, line 75) is available.
- `models.py`: `DictColumnsIndexes` (lines 146-151): action=0, match=1, search=2, term=4 → min 5 columns.
- No existing tests cover any of the four error scenarios.

## Global constraints

- C1. New exception `DictionaryStructureError` carries: `file_path`, `expected_min_columns`,
  `actual_columns`, `hint`, `step`. Rich multi-line `__str__`.
- C2. Primary validation in `CSVWorker.get_dictionary()` (owns `self.dict_path`); defense-in-depth
  in `MappingDict._initialize_mapping` (add optional `dict_path` param to `__init__`).
- C3. `MappingDict.__init__` gains optional `dict_path: Path | None = None`; stored as `self._dict_path`;
  threaded from `app_service.run_report` (`dict_path=self.dict_path`).
- C4. `build_match_query` guards `columns_list[col_idx]` with bounds check → `DictionaryStructureError`.
- C5. **No changes to `app.py`** — the rich `DictionaryStructureError.__str__` flows through the
  existing `except Exception as e: console.print(f"❌ Ошибка: {e}")` handler.
- C6. Out of scope: auto-detecting/fixing the separator; i18n changes; file-format changes.
- C7. Tests verify logic and interaction, not implementation details.
- C8. Local gates: `uv run pytest`, `uv run ruff check src/ tests/`, `uv run basedpyright src/`.

## Block decomposition (dependency order)

| Block | Summary | Depends on | Risk | Agent |
|---|---|---|---|---|
| B1 | `DictionaryStructureError` + `MappingDict` validation + `build_match_query` guard + `CSVWorker.get_dictionary` validation + `dict_path` threading + all tests | — | Low | Implementor |
| DOC | Update documentation affected by the new error type and validation | B1 | Low | Doc-specialist |
| VAL | Final validation of completed implementation | B1, DOC | Low | Validator |

**Skipped agents (justification):** Auditor (already completed — `.ai/tmp/code_context.md`),
Researcher (no technical uncertainty; auditor verified all proposed approaches against source),
Planner (plan is sufficiently detailed; path inaccuracies corrected in this execution plan),
Product Owner (no business uncertainty — error-handling only, no product-behavior change).

---

## B1 — Dictionary structure error reporting (LOW RISK)

### {task_description} for Implementor

```yaml
id: task_020_b1

title: Raise informative DictionaryStructureError on dictionary/data column mismatch

priority: high

depends_on: []

source_reference: .ai/problems/04_uninformative_error_on_dictionary_column_mismatch.md
source_section: §6 — Proposed solution directions (A–E); §8 test strategy

description: >
  When a dictionary CSV uses a separator that does not match
  read_settings.from_csv.separator, Polars reads it as a 1-column DataFrame and
  MappingDict._initialize_mapping raises a bare "IndexError: index 1 is out of
  bounds for sequence of length 1" with zero actionable context. Add a
  DictionaryStructureError exception carrying file path, expected/actual column
  counts, a separator-mismatch hint, and the pipeline step; add validation at the
  CSVWorker.get_dictionary boundary (primary, has dict_path) and in
  MappingDict._initialize_mapping (defense-in-depth, optional dict_path param);
  guard build_match_query against out-of-range column index; thread dict_path
  from run_report. No changes to app.py — the rich __str__ flows through the
  existing catch-all handler.

goals:
  - users see file path, step, expected/actual column count, and separator hint
  - bare IndexError never reaches the user for this class of bug
  - existing behavior preserved for well-formed dictionaries
  - all existing tests pass; new tests cover each validation path

files:
  - path: src/mko_data_cleaner/core/errors.py
    targets:
      - type: class
        name: DictionaryStructureError
  - path: src/mko_data_cleaner/core/dict_service.py
    targets:
      - type: class
        name: MappingDict
      - type: method
        name: __init__
      - type: method
        name: _initialize_mapping
      - type: method
        name: build_match_query
  - path: src/mko_data_cleaner/core/csv_service.py
    targets:
      - type: class
        name: CSVWorker
      - type: method
        name: get_dictionary
      - type: method
        name: _validate_dictionary_columns
  - path: src/mko_data_cleaner/core/app_service.py
    targets:
      - type: method
        name: run_report
  - path: tests/conftest.py
    targets:
      - type: fixture
        name: separator_mismatch_report
  - path: tests/test_dict_service.py
  - path: tests/test_csv_service.py
  - path: tests/test_app.py

changes:
  - action: add_error_type
    file: src/mko_data_cleaner/core/errors.py
    description: >
      Add DictionaryStructureError(Exception) with fields file_path,
      expected_min_columns, actual_columns, hint, step. __str__ produces a
      multi-line message: title, File, Step, column-count issue, Actual columns,
      Hint. Follows the SettingsNotInitializedError pattern (custom __init__).
    code_hint: |
      class DictionaryStructureError(Exception):
          def __init__(
              self,
              file_path: Path | str | None = None,
              expected_min_columns: int = 0,
              actual_columns: list[str] | None = None,
              hint: str | None = None,
              step: str | None = None,
          ):
              self.file_path = file_path
              self.expected_min_columns = expected_min_columns
              self.actual_columns = actual_columns or []
              self.hint = hint
              self.step = step
              parts = ["Dictionary structure validation failed"]
              if file_path:
                  parts.append(f"File: {file_path}")
              if step:
                  parts.append(f"Step: {step}")
              parts.append(
                  f"Dictionary has {len(self.actual_columns)} column(s) but "
                  f"col_indexes references column index {expected_min_columns - 1} "
                  f"(needs at least {expected_min_columns} columns)."
              )
              if self.actual_columns:
                  parts.append(f"Actual columns: {', '.join(self.actual_columns)}")
              if hint:
                  parts.append(f"Hint: {hint}")
              super().__init__("\n  ".join(parts))

  - action: add_dict_path_to_mapping_dict
    file: src/mko_data_cleaner/core/dict_service.py
    description: >
      Add optional dict_path: Path | None = None parameter to MappingDict.__init__
      (after action_col_indexes). Store as self._dict_path before _initialize_mapping().
      Import DictionaryStructureError and Path.

  - action: validate_columns_in_initialize_mapping
    file: src/mko_data_cleaner/core/dict_service.py
    description: >
      In _initialize_mapping, after computing action_idx, check that
      self.data.width > max(action_idx). If not, raise DictionaryStructureError
      with self._dict_path, expected_min_columns=max_idx+1, actual_columns=self.data.columns,
      step='building mapping rules'. When width==1, add separator-mismatch hint.
    code_hint: |
      action_idx = list(self.action_col_indexes.values())
      max_idx = max(action_idx)
      if self.data.width <= max_idx:
          hint = None
          if self.data.width == 1:
              hint = (
                  "The dictionary was read as a single column — the CSV separator "
                  "in 'read_settings.from_csv.separator' may not match the "
                  "dictionary file's actual separator."
              )
          raise DictionaryStructureError(
              file_path=self._dict_path,
              expected_min_columns=max_idx + 1,
              actual_columns=self.data.columns,
              hint=hint,
              step="building mapping rules",
          )

  - action: guard_build_match_query
    file: src/mko_data_cleaner/core/dict_service.py
    description: >
      In build_match_query, before columns_list[col_idx], check col_idx bounds
      (0 <= col_idx < len(columns_list)). If out of range, raise
      DictionaryStructureError with self._dict_path, expected_min_columns=col_idx+1,
      actual_columns=columns_list, step='building match query', and a hint about
      the data-column / search-index mismatch.
    code_hint: |
      if col_idx < 0 or col_idx >= len(columns_list):
          raise DictionaryStructureError(
              file_path=self._dict_path,
              expected_min_columns=col_idx + 1,
              actual_columns=columns_list,
              hint=(
                  f"Dictionary references data column index {col_idx} but the "
                  f"data table only has {len(columns_list)} columns. Check that "
                  "the 'search' values in the dictionary match your data file's "
                  "column layout."
              ),
              step="building match query",
          )

  - action: validate_dictionary_in_get_dictionary
    file: src/mko_data_cleaner/core/csv_service.py
    description: >
      In get_dictionary, after pl.read_csv, call self._validate_dictionary_columns(df).
      Add _validate_dictionary_columns method: read col_indexes from
      self.dict_settings (guard empty), compute max_idx, if df.width <= max_idx
      raise DictionaryStructureError with self.dict_path, step='reading dictionary'.
      When width==1, include configured separator in the hint.
    code_hint: |
      def get_dictionary(self) -> pl.DataFrame:
          if not self.dict_path.is_file():
              self.get_merged_dictionary()
          df = pl.read_csv(self.dict_path, **self.reader_settings)
          self._validate_dictionary_columns(df)
          return df

      def _validate_dictionary_columns(self, df: pl.DataFrame) -> None:
          col_indexes = self.dict_settings.get("col_indexes")
          if not col_indexes:
              return
          max_idx = max(col_indexes.values())
          if df.width <= max_idx:
              hint = None
              if df.width == 1:
                  configured_sep = self.reader_settings.get("separator", "None")
                  hint = (
                      f"The dictionary was read as a single column — the CSV separator "
                      f"in 'read_settings.from_csv.separator' ('{configured_sep}') may "
                      f"not match the dictionary file's actual separator."
                  )
              raise DictionaryStructureError(
                  file_path=self.dict_path,
                  expected_min_columns=max_idx + 1,
                  actual_columns=df.columns,
                  hint=hint,
                  step="reading dictionary",
              )

  - action: thread_dict_path_in_run_report
    file: src/mko_data_cleaner/core/app_service.py
    description: >
      In run_report, pass dict_path=self.dict_path to the MappingDict constructor.
    code_hint: |
      mapping_dict = MappingDict(
          data=df,
          action_col_indexes=self.app_config.dict_file_settings.col_indexes,
          dict_path=self.dict_path,
      )

  - action: add_test_mapping_dict_structure_error
    file: tests/test_dict_service.py
    description: >
      Unit test: MappingDict with a 1-column DataFrame raises DictionaryStructureError
      (not IndexError); message contains the dict_path and 'separator' hint.

  - action: add_test_build_match_query_out_of_range
    file: tests/test_dict_service.py
    description: >
      Unit test: build_match_query with an out-of-range col_idx raises
      DictionaryStructureError with a descriptive message.

  - action: add_test_get_dictionary_separator_mismatch
    file: tests/test_csv_service.py
    description: >
      Unit test: CSVWorker.get_dictionary on a comma-separated dict with ';' config
      raises DictionaryStructureError mentioning the file and separator.

  - action: add_separator_mismatch_fixture_and_integration_test
    file: tests/conftest.py
    description: >
      Add separator_mismatch_report fixture (comma-separated dict, ';'-separated data).
    file2: tests/test_app.py
    description2: >
      Integration test: process_data on separator_mismatch_report raises
      DictionaryStructureError whose message contains 'merged_dictionary.csv'
      and 'separator'. CLI test: runner.invoke run on the same report → exit_code 1
      and output contains the file path and separator hint.

acceptance_criteria:
  - DictionaryStructureError defined in errors.py with file_path, expected_min_columns,
    actual_columns, hint, step fields
  - MappingDict._initialize_mapping raises DictionaryStructureError (not IndexError)
    when data has fewer columns than col_indexes requires
  - CSVWorker.get_dictionary raises DictionaryStructureError on separator mismatch
    with the dict file path and a separator hint
  - build_match_query raises DictionaryStructureError with a clear message on
    out-of-range col_idx
  - DictionaryStructureError.__str__ includes the step label (e.g. 'reading dictionary')
  - process_data on a comma-separated dict report raises DictionaryStructureError
    mentioning merged_dictionary.csv and separator
  - CLI `run` prints '❌ Ошибка' with the dictionary file path and separator hint
  - all existing tests pass (uv run pytest)
  - uv run ruff check src/ tests/ is clean
  - uv run basedpyright src/ reports no new type errors in modified files

verification:
  - uv run pytest tests/test_dict_service.py tests/test_csv_service.py tests/test_app.py -v
  - uv run pytest tests/ -v
  - uv run ruff check src/ tests/
  - uv run basedpyright src/
```

## DOC — Documentation (after B1)

Doc-specialist updates only documentation affected by this change:
- `docs/00-overview/architecture.md` — if it lists error-handling patterns or the dictionary
  validation boundary, update to mention `DictionaryStructureError` and early validation in
  `CSVWorker.get_dictionary`.
- `docs/00-overview/doc-maintenance-rules.md` — no change expected (rules file).
- Commit separately: `docs(core): document DictionaryStructureError validation`

## VAL — Final validation (after B1, DOC)

Validator checks: plan completeness, cross-block integration, architectural consistency,
regressions, tests and quality gates, documentation accuracy, no unrelated changes.
