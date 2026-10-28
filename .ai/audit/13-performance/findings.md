---
phase: "13"
phase_name: "performance"
id_prefix: "PERF"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/13-audit-performance.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Performance & Scalability

## Summary

Four findings survive verification against the current tree (`c0fd0e1`), spanning MEDIUM→LOW. The highest-severity finding (PERF-001) concerns the CLI discarding per-stage latency attribution that the pipeline already measures and tests. Two findings from the phase task file (PERF-005, PERF-006) are **already resolved** in the current tree — the phase's `code_ref` (`384044a`) predates their fix commits. No CRITICAL or HIGH gaps were found; the single-worker pipeline has no retry/backoff (by design) and cleanup is fail-safe via the `DBWorker` context manager.

## Scope

**Examined:** `src/mko_data_cleaner/app.py` (`run`, `process_data`), `src/mko_data_cleaner/core/app_service.py` (`run_report`, `_import_data`), `src/mko_data_cleaner/core/csv_service.py` (`CSVWorker`, `export_sql_to_csv`), `src/mko_data_cleaner/core/db_service.py` (`DBWorker._apply_delete`, `_init_base`), `src/mko_data_cleaner/core/models.py` (`ReportResult`, `PolarsWriteCSV`), `src/mko_data_cleaner/settings/app_config.json5`, docs in `docs/00-overview/`, tests in `tests/test_app.py`, `tests/test_csv_service.py`, `tests/test_db_service.py`.

**Tools:** `git` (log/diff/grep), `ruff` (lint — all checks passed), `pytest` (93 passed), built-in `grep` tool.

**Not verified:** End-to-end latency at production data volume (no production-scale dataset available; project has no Docker/external services). Per-stage timings are measured and returned in-memory but not surfaced to the operator (PERF-001).

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| PERF-001 | MEDIUM | `src/mko_data_cleaner/app.py::run` | CLI prints total `duration` but discards per-stage `timings` dict |
| PERF-002 | LOW | `docs/00-overview/current-behavior.md::"process_data"` | Docs claim `process_data` returns `None`; code returns `ReportResult` |
| PERF-003 | LOW | `.kilo/commands/audit/phases/13-audit-performance.md::"PERF-"` | Phase-file prefix-collision warning is phantom (zero `PERF-` IDs in tree) |
| PERF-004 | LOW | `src/mko_data_cleaner/core/csv_service.py::CSVWorker.CHUNK_SIZE` | Read chunk (10 000) is hardcoded; export chunk (100 000) is config-driven; model default (10 000) is redundant |

#### PERF-001 — [MEDIUM] CLI discards per-stage timings

**Anchor:** `src/mko_data_cleaner/app.py::run`
**Disambiguator:** `result.duration`
**Checked:** ran
**Why this band:** MEDIUM per phase taxonomy — "a measurement asserted but not emitted": the data is measured and tested but never surfaced to the operator.
**Problem:** The CLI `run` command (app.py) prints only `result.duration` to the console. The three per-stage timings (`import`, `mapping`, `export`) are measured by `run_report` via `perf_counter` deltas, stored in a `timings` dict, and returned in `ReportResult.timings` — yet the CLI never renders them.
**Repro:** 1) `uv run pytest tests/test_app.py::test_process_data_past_gate_does_not_raise_settings_error -q` → pass (E2E test confirms `result.timings` has keys `{"import","mapping","export"}` at test_app.py:239). 2) `uv run mko-data-cleaner run <report>` → console prints only total duration; no stage breakdown. 3) `app.py:81-84` formats only `result.duration`; `result.timings` (measured at `app_service.py:175-177, 241, 269`) is never rendered.
**Impact:** An on-call developer diagnosing a slow run cannot distinguish whether time is spent in CSV import, the FTS5/LIKE mapping pass, or CSV export — all three are conflated into one total.
**Root cause:** CLI output formatting in `app.py::run` omits `result.timings` from the success message.
**Fix:** Append per-stage timing breakdown (import / mapping / export seconds) to the CLI success string in `run`, drawing from `result.timings`.
**Verify:** `tests/test_app.py:239` (`assert set(result.timings.keys()) == {"import", "mapping", "export"}`) validates the contract; extend to assert the CLI renders the breakdown.
**Related:** PERF-002 (same `ReportResult` return path).

**Evidence — `read src/mko_data_cleaner/app.py` lines 79-89** *(supports: CLI omits timings)*:
```text
    try:
        result = service.run_report(report)
        console.print(
            f"[green]✅ Report complete:[/green] "
            f"{result.rows_loaded:,} loaded → {result.rows_exported:,} clean "
            f"({result.duration:.1f}s) ↘ {result.export_dir}"
        )
```

**Evidence — `read src/mko_data_cleaner/app_service.py` lines 112, 175-177, 280-292** *(supports: timings measured and returned)*:
```text
112|        t_start = perf_counter()
...
175|        t_import = perf_counter()
176|        rows_loaded = self._import_data(db_worker, csv_worker)
177|        timings["import"] = perf_counter() - t_import
...
280|        return ReportResult(
...
289|            timings=timings,
290|            rows_loaded=rows_loaded,
291|            rows_exported=rows_exported,
```

#### PERF-002 — [LOW] Docs claim `process_data` returns `None`

**Anchor:** `docs/00-overview/current-behavior.md::"process_data"`
**Disambiguator:** `None`
**Checked:** ran (static + test)
**Why this band:** LOW per phase taxonomy — "drift and reproducibility gaps, no runtime consequence"; docs are stale but code and tests are correct.
**Problem:** Three documentation sites in `docs/00-overview/` assert that `process_data` returns `None` ("the result object is owner-confirmed but not implemented"). In fact the code has returned a fully-populated `ReportResult` (11 fields, including `timings`, `duration`, row counts) since the implementation; two tests assert `isinstance(result, ReportResult)`.
**Repro:** 1) `uv run pytest tests/test_app.py::test_process_data_returns_report_result tests/test_app.py::test_report_result_model_construction_and_serialization -q` → 2 passed. 2) Code path: `app.py:process_data` (line 128) → `get_app_service(...).run_report(path)` (line 145) → returns `ReportResult(...)` (app_service.py:280-292).
**Impact:** Notebook and Airflow consumers following the docs may skip inspecting `result.timings` / `result.duration`, believing no structured data is available.
**Root cause:** Docs in `current-behavior.md` (lines 272-275), `architecture.md` (line 219), and `data-flow.md` (line 165) were not updated when `ReportResult` was implemented (commit 340fb1c).
**Fix:** Update the three doc sites to state `process_data` returns `ReportResult` with paths, timings, and row counts; remove the "not implemented" qualifier.
**Verify:** `tests/test_app.py:235` (`assert isinstance(result, ReportResult)`), `tests/test_app.py:260-283` (model construction + serialization).
**Related:** PERF-001 (same `ReportResult` return path).

**Evidence — `read docs/00-overview/current-behavior.md` lines 272-275**:
```text
Note: `process_data` currently returns `None`; the result object (paths +
timings + row counts) is owner-confirmed but not implemented — see
[product-overview.md](product-overview.md) and
[open-questions.md](open-questions.md) D-6 [runtime-verified].
```

#### PERF-003 — [LOW] Phantom PERF- prefix-collision warning

**Anchor:** `.kilo/commands/audit/phases/13-audit-performance.md::"PERF-001"`
**Disambiguator:** `PERF-`
**Checked:** ran (git grep)
**Why this band:** LOW — documentation/meta drift with no runtime consequence; the warning is simply false.
**Problem:** The phase file (line 160) warns that the `PERF-` ID prefix may collide with existing identifiers: "repo-wide grep confirms zero `PERF-NNN`… The collision warning is phantom." A repo-wide search confirms: there are zero `PERF-` identifiers anywhere in the tree outside this phase file. The warning is self-defeating.
**Repro:** `git grep -n "PERF-" -- ':!:.kilo/commands/audit'` → no output (empty result).
**Impact:** None. No naming collision exists.
**Root cause:** Stale warning template carried over from a prior phase's (Phase 10) disproven `QLT-` collision pattern.
**Fix:** No code change; the phase file can retain its `PERF-` prefix without reservation.
**Verify:** `git grep PERF-` (see Appendix A).
**Related:** PERF-005, PERF-006 (phase-template findings already resolved).

## Fix Order

- **Dependencies:** PERF-002 and PERF-001 share the `ReportResult` return path; PERF-001's CLI fix depends only on `result.timings` being present (already the case). `None.` — no ordering dependency.
- **Merge candidates:** None — four independent fixes.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** None.
- **Rollout risk:** PERF-001 CLI change is cosmetic; must keep `test_app.py:238-239` green (contract unchanged). PERF-002 is docs-only. PERF-003 is a no-op. PERF-004 requires config-driven read chunk size; must keep `test_csv_service.py:114,158` green.

## Appendices

### Appendix A — PERF-003: repo-wide `PERF-` grep (supports: "zero PERF- identifiers")

```text
> git grep -n "PERF-" -- ':!:.kilo/commands/audit'
(no output)
```

### Appendix B — PERF-004: chunk-size configuration vs. code (supports: "read 10 000, export 100 000")

```text
> src/mko_data_cleaner/core/csv_service.py:28   →  CHUNK_SIZE = 10000
> src/mko_data_cleaner/core/csv_service.py:315  →  max_rows = params.pop("chunk_size", 10000)
> src/mko_data_cleaner/settings/app_config.json5:40 →  "chunk_size": 100000
> src/mko_data_cleaner/core/models.py:90       →  chunk_size: NonNegativeInt = 10000
```

### Appendix C — Resolved findings PERF-005 and PERF-006 (verified, not active)

The phase task file lists PERF-005 (`DATA_TO_SQL_PARAMS` + `data_chunk_to_sql` dead code at `db_service.py:22-27, 505-520`) and PERF-006 (`_apply_delete` double DELETE at `db_service.py:630-643`). Both were valid at the phase's `code_ref` (`384044a`) but are **already resolved** in the current tree (`c0fd0e1`):

- **PERF-005:** Removed by commit `7aa4f58` ("chore(core): remove dead pandas to_sql residue") — 29 lines deleted from `db_service.py`. The `grep` tool finds zero references to `DATA_TO_SQL_PARAMS` or `data_chunk_to_sql` in `src/mko_data_cleaner/`.
- **PERF-006:** Fixed by commit `4af9813` ("fix(core): execute DELETE once in DBWorker._apply_delete") — the duplicate `self.perform_query(sql)` line was removed. Current `_apply_delete` (db_service.py:648-660) executes the DELETE exactly once. Regression test `test_apply_delete_executes_delete_once` added at `tests/test_db_service.py:404`. Docs updated at commit `a66b876`.

`git log --oneline 384044a..c0fd0e1 -- src/mko_data_cleaner/core/db_service.py` confirms both commits are in the range between the phase's code_ref and HEAD.

### Appendix D — Phase-file code_ref drift (supports: stale citations)

The phase file carries `code_ref: "384044a"` but the audit base is `c0fd0e1` (HEAD; `384044a` is a confirmed ancestor). Three category of drift:

1. **Config format:** phase cites `app_config.yaml:42`; config migrated to JSON5 (`app_config.json5:40`) at commit `91e963b`.
2. **Polars API:** phase cites `pl.read_csv_batched(batch_size=...)` at `csv_service.py:170-172`; code now uses `pl.scan_csv(...).collect_batches(chunk_size=...)` (migrated at `4d5d35e`).
3. **Test reference:** phase cites `test_csv_service.py:45` for `chunk_size: 10000`; line 45 is actually about `_is_date_column` — a stale line citation.

These do not invalidate findings PERF-001–PERF-004 (the underlying constants and behavior are unchanged), but they explain why PERF-005 and PERF-006 could not be reproduced: the code they reference no longer exists.
