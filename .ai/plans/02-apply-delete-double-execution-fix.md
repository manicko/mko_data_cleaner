# Execution Plan 02 — Fix `DBWorker._apply_delete` double DELETE execution

- **Status:** Complete (implemented 2026-10-15; fix `4af9813`, docs `a66b876`; validation PASSED — no required fixes)
- **Created:** 2026-10-15 by Tech Lead (multi-agent workflow)
- **Source problem note:** `.ai/audit/problems/01-apply-delete-double-execution.md`
- **Code context (Auditor):** `{code_context}` returned by the Auditor agent, 2026-10-15 (key facts summarized below; no audit-phase files were modified)
- **Baseline HEAD:** `cd12215`
- **Recommended fix (from problem note, verified safe by audit):** Option A — remove the second `self.perform_query(sql)` call in `_apply_delete`; keep the `logger.debug` line

## Audit verdict (Step 0)

- The defect exists exactly as documented: `DBWorker._apply_delete` in
  `src/mko_data_cleaner/core/db_service.py` builds one
  `DELETE FROM {target_table} WHERE rowid IN (SELECT DISTINCT data_rowid FROM {joined_matches_table})`
  statement and executes it twice via `self.perform_query(sql)`, with
  `logger.debug(f"Apply delete rules to {self.target_table}")` between the calls.
  Only cosmetic line-number drift in the problem note (`perform_query` at 224–235,
  `_apply_delete` at 659–672, not 195–206 / 630–643).
- The duplicate-execution pattern is **unique** to `_apply_delete` (verified across the module).
- Zero break-risk: no test mocks/spies/asserts `perform_query` call counts; the only
  end-to-end DELETE assertion (`tests/test_app.py::test_process_data_end_to_end`,
  `rows_exported == 2`) is on observable outcome and is unchanged by the fix.
- No transaction/atomicity wrapper exists to respect: each `perform_query` commits its own
  implicit transaction; ADBC (`db_adb_con`) commits are a separate, untouched path.

## Global constraints

- C1. Remove **only** the second `self.perform_query(sql)` call in `_apply_delete`
  (the one after the `logger.debug` line). Keep the debug log verbatim.
- C2. Behavior-preserving: rows matched via `joined_matches_table` must still be deleted
  exactly once. No other production-code changes in this block.
- C3. Add exactly one focused regression test (see B1). No unrelated test refactors.
- C4. Do **not** touch documentation (`docs/`, README) — the Doc-specialist handles
  `docs/00-overview/architecture.md` and `docs/00-overview/open-questions.md` D-10
   in the Documentation step. (`docs/00-overview/doc-maintenance-rules.md`
  does not exist in this repo; project rule 14 applies.)
- C5. Do **not** modify `.ai/` plan/audit/problem files.
- C6. Commit only the files of the current work item, conventional-commit style
  (repo style: `fix(core): ...`). No `git reset`/`checkout`, no history rewrite.
- C7. Validation gate (local, this repo — standalone uv project, no Docker):
  `uv run pytest tests/` (targeted first: `tests/test_db_service.py`),
  `uv run ruff check <changed files>`, `uv run mypy <changed files>`.

## Block decomposition (dependency order)

| Block | Summary | Depends on | Risk | Agents |
|---|---|---|---|---|
| B1 | Remove duplicate `perform_query` call in `_apply_delete` + regression test | — | Low | Implementor |
| DOC | Update defect status in `architecture.md` + `open-questions.md` D-10 | B1 | Low | Doc-specialist |
| VAL | Final validation of the completed change | B1, DOC | Low | Validator |

**Skipped agents (justification):** Researcher (problem note already evaluated
options A/B/C; audit confirmed no technical uncertainty), Planner (plan is a single
atomic change with a chosen option — sufficiently detailed to proceed directly),
Product Owner (no business uncertainty; user-visible behavior is unchanged).

## Per-block task records

(Format follows `.ai/tasks/templates/task_template.yaml` — semantic code units only.)

---

## B1 — Remove duplicate DELETE execution (LOW RISK)

### {task_description} for Implementor

```yaml
id: task_02_b1

title: Execute DELETE exactly once in DBWorker._apply_delete

priority: medium

depends_on: []

source_reference: .ai\audit\problems\01-apply-delete-double-execution.md

source_section: "Options — Option A (recommended)"

description: >
  DBWorker._apply_delete builds one DELETE statement and executes it twice in a
  row via perform_query, each with its own commit. The second execution can never
  match rows because the first already removed them — dead work and misleading.
  Remove the second self.perform_query(sql) call; keep the logger.debug line.
  Add a regression test that fails if the duplication is reintroduced.

goals:
  - execute the DELETE statement exactly once per _apply_delete call
  - preserve observable behavior (matched rows deleted exactly once)
  - keep debug logging intact and consistent with _apply_replace/_apply_add
  - prevent regression via a focused test

files:
  - path: src/mko_data_cleaner/core/db_service.py
    targets:
      - type: class
        name: DBWorker
      - type: method
        name: _apply_delete
  - path: tests/test_db_service.py
    targets:
      - type: test
        name: test_apply_delete_executes_delete_once (new test)

changes:
  - action: remove_code
    description: >
      In DBWorker._apply_delete, delete the second self.perform_query(sql) call
      (the one following the logger.debug line). Keep the single execution before
      the debug log, keep the SQL statement construction unchanged, keep
      logger.debug(f"Apply delete rules to {self.target_table}") verbatim.
  - action: add_code
    description: >
      Add regression test in tests/test_db_service.py using the existing
      db_worker fixture (tests/conftest.py): spy on perform_query
      (pytest-mock mocker.spy), set up target table rows plus a joined_matches_table
      (TEMP table bound to the worker's connection, use_temp_tables=True default)
      whose data_rowid values reference target rowids, call _apply_delete(),
      and assert (a) perform_query was called exactly once, (b) the call executed
      the DELETE statement (sql starts with DELETE FROM {target_table}),
      (c) the matched target rows are gone and unmatched rows remain.

acceptance_criteria:
  - _apply_delete executes perform_query exactly once; logger.debug retained
  - regression test fails against the pre-fix code and passes after the fix
  - uv run pytest tests/test_db_service.py passes
  - uv run pytest tests/ passes (full suite)
  - uv run ruff check src/mko_data_cleaner/core/db_service.py tests/test_db_service.py clean
  - uv run mypy src/mko_data_cleaner/core/db_service.py tests/test_db_service.py reports no new errors
  - no unrelated changes; docs untouched in this block
  - commit only the two changed files: fix(core): execute DELETE once in DBWorker._apply_delete
```

---

## DOC — Documentation status update (after B1)

Doc-specialist updates only the two defect-status entries made stale by the fix:

- `docs/00-overview/architecture.md` — the "Duplicate execution of the DELETE statement
  in `_apply_delete` … noted; not fixed" note (semantic anchor: the D-10 defect note
  near the end of the file) → mark as fixed, referencing this plan.
- `docs/00-overview/open-questions.md` — discrepancy D-10 entry → status "Fixed",
  referencing Execution Plan 02 and the fix.

Committed separately: `docs(core): mark _apply_delete double-execution defect fixed`.

## VAL — Final validation (after DOC)

Validator checks: plan completeness, the single change is minimal and behavior-preserving,
regression test quality (fails pre-fix / passes post-fix), full test suite green,
lint/type gates, documentation accuracy vs. code, no unrelated changes.
