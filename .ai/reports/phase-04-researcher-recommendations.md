---
phase: "04"
phase_name: "Data Pipeline & Report Lifecycle"
target_file: ".kilo/commands/audit/phases/04-audit-data-pipeline.md"
author: researcher
evidence_source: >
  src/mko_data_cleaner/{app.py,core/app_service.py,core/csv_service.py,core/db_service.py,core/dict_service.py,core/models.py,core/errors.py,core/utils.py}
  + docs/00-overview/{architecture.md,data-flow.md,open-questions.md}
  + phase auditor reports (01, 02, 03, 09, 13)
verdict: REBASE
confidence: HIGH
---

# Phase 04 — Researcher Recommendations for Refining the Audit Spec

The auditor's report (`phase-04-auditor-report.md`) verdict is **REBASE**: Phase 04 is the data-pipeline core of `mko_data_cleaner`, and every auditor finding below is code-verified against the working tree. This document endorses the auditor's proposed 6-block set with minor refinements, re-bases the severity bands and cross-phase citations, and adds two doc-vs-code discrepancies the Phase 04 file should surface (gap #8 is reachable, not hypothetical; "empty `__init__.py`" is stale).

## 0. Evidence Note (read before editing)

All line references below are spot-checked against `src/mko_data_cleaner/` at the time of writing. The canonical run chain is `AppService.run_report` (`app_service.py:110-290`) → `CSVWorker` / `MappingDict` / `DBWorker`. The 14-step pipeline in the auditor report matches the source exactly. Two doc-vs-code gaps emerged that tighten two of the auditor's eight candidate findings:

- **`process_data` returns `ReportResult`, not `None`.** `app.py:119-138` declares `-> ReportResult` and `return get_app_service(settings_dir).run_report(path)`. The docs `data-flow.md:165` ("Currently `None`") and `architecture.md:218` ("returns None (result object: target work)") and `open-questions.md D-6` are **stale**. This makes gap #8 (no error field) *reachable* — a caller can read `rows_exported == 0` and mistake an empty run for a successful one.
- **"Repeated runs never overwrite"** (`data-flow.md:178`) is **contradicted** by `csv_service.py:239` (`strftime("%Y%m%d-%H%M%S")`, second granularity). This strengthens gap #4 from a reproducibility nit to a data-loss class.

## 1. Proposed Final Block Set (re-based, renumbered 1–6)

Endorse the auditor's 6-block set verbatim in numbering. It maps every surviving concern onto the synchronous pipeline with no phantom concepts. Refinements (evidence only; do not add/remove/merge blocks):

| # | Title | Refinement vs. auditor draft |
|---|---|---|
| 1 | Run input scope & boundary detection | Add: `get_dictionary` merge-on-miss path (`csv_service.py:203-206`) is part of scope — a missing/renamed `merged_dictionary.csv` triggers a re-merge. Evidence request extends to that branch. |
| 2 | Mapping rule-block generation & application ordering | **New link:** the double-DELETE (gap #1) and the REPLACE-then-ADD overwrite (open-questions D-2) are both rule-application-ordering defects and should be provoked *within* this block, not filed separately. |
| 3 | Pipeline stage ordering & data-flow correctness | Tighten the "wrong table" check: `target_table` (`db_service.py:76-79`) returns `_index_tbl_name` when `index_column` is set, else `data_tbl_name`. Verify the no-`index_column` fallback is intentional, not a latent mis-target. |
| 4 | DataFrame round-trip fidelity & empty-result handling | Confirm `schema_overrides` forces `Utf8` on export (`csv_service.py:325`) and that `clean_names` (`utils.py:81-101`) + `make_valid` (`utils.py:73-78`) are the only name sanitizers in the chain. |
| 5 | Export path resolution, naming, & crash-safety | Extend evidence: (a) `data-flow.md:178`'s "never overwrite" claim vs. `csv_service.py:239`; (b) the `file_index` scheme starts at 1 per run (does not disambiguate cross-run). |
| 6 | Failure mode, crash-state preservation & observability | Extend evidence: (a) `app.py:138` returns `ReportResult` (gap #8 is reachable, see §5); (b) the DB delete on exit is *documented* as intended in `architecture.md:136-137` and `data-flow.md:36` — so this is a design-intent gap, not a regression. |

### Block 1 — Run input scope & boundary detection
**Angle:** From `data_path`, establish what is consumed and how the run fails loud on missing input.
- File discovery: `CSVWorker._set_files_params` (`csv_service.py:60-75`) enumerates `import_folder` for the configured extension; empties → `WrongDataSettings`. Confirm `list_files_in_directory` (`utils.py:104-128`) is non-recursive and extension-gated.
- Header read: `get_csv_headers` (`csv_service.py:77-86`) uses `pl.read_csv(n_rows=0)` — verify only the first file's header is taken and the rest are assumed identical.
- Date detection: `check_date_column`/`_detect_date_column` (`csv_service.py:124-157`) apply `DATE_REGEX = r"^\d{4}[-/.]\d{2}[-/.]\d{2}$"` (`csv_service.py:29`) over a 500-row sample at 0.8 threshold; the docstring claims `DD/MM/YYYY` support that the regex does not provide (open-questions D-4).
- **Evidence:** the import file list + detected date column; one empty `raw_data/` (must raise `WrongDataSettings`); one sample-based mis-detect (e.g. a non-date column that still matches the regex ≥80%).

### Block 2 — Mapping rule-block generation & application ordering
**Angle:** Verify the fixed block order and the per-block write→apply cycle, and provoke the two ordering defects.
- `MappingDict.generate_rules_blocks` (`dict_service.py:79-94`) yields `DELETE → REPLACE → ADD`; REPLACE is further split by `group_by_cols` (`dict_service.py:63-77`) into column-shape sub-blocks. The caller loop (`app_service.py:217-239`) writes each block to `mapping_table` (ADBC `replace`) then calls `db_worker.apply_mapping(...)`.
- **Provokable defects to surface here:**
  - Gap #1: `_apply_delete` (`db_service.py:630-643`) executes the *same* `DELETE` twice (lines 640, 643) with a `logger.debug` between — second pass matches nothing. Prove via `SELECT changes()` after each.
  - D-2 (open-questions): REPLACE-then-ADD on one column overwrites the REPLACE value instead of merging (`_apply_add`, `db_service.py:674-729`). Already owner-confirmed-intent; document the discrepancy.
- **Evidence:** the block sequence with line refs; one induced DELETE block and two `changes()` values (N then 0); one REPLACE-then-ADD row and the cell content vs. the documented `1 2` expectation.

### Block 3 — Pipeline stage ordering & data-flow correctness
**Angle:** Verify the stage sequence in `run_report` (`app_service.py:126-268`) and the invariants the docs assert.
- Required invariants: rules applied *before* the clean-data export (`app_service.py:264-268` follows the `generate_rules_blocks` loop at `:217-239`); non-mapped extracted *before* sync (`build_non_mapped` at `:244` precedes `sync_with_data_table` at `:262`); FTS vs LIKE insertion gated on `is_empty()` (`:171,181,196,211`).
- Match-insertion targets: `insert_matches_from_fts` queries the FTS virtual table (`db_service.py:541-554`); `insert_matches` (`db_service.py:557-586`) scans `self.target_table` — confirm which table that resolves to for the no-`index_column` case.
- **Evidence:** the stage order with line refs; one invariant and its static proof; one redundant computation (e.g. `full_matches_table` rebuilt per block via `_build_joined_matches` at `db_service.py:588-603`).

### Block 4 — DataFrame round-trip fidelity & empty-result handling
**Angle:** Trace column identity and emptiness from CSV header to exported CSV.
- Chain: `source_headers` → `set_data_tbl_columns` (`db_service.py:107-110`, via `clean_names`) → `MappingDict.build_mapping` (`dict_service.py:119-142`, renames extra cols) → SQLite (`TEXT` all, `create_table`) → `export_sql_to_csv` cursor→`pl.DataFrame` with `schema_overrides = {col: pl.Utf8}` (`csv_service.py:325`).
- Empty path: `export_sql_to_csv` returns `0` when `total_rows == 0` (`csv_service.py:334-336`); `run_report` still returns a `ReportResult` with `rows_exported == 0` (gap #8).
- **Evidence:** the column-name chain for one report (incl. one sanitized name, e.g. `COST RUB` → `COSTRUB`); one empty table and its return; one non-fatal header/export mismatch.

### Block 5 — Export path resolution, naming & crash-safety
**Angle:** Audit where exports land, how they're named, and whether a crash leaves half-written files.
- Path asymmetry: null_data → `f"{self.base_path}"` (`app_service.py:249`, report root); clean_data → `self.export_path` (`app_service.py:267`, `base_path/clean_data`). Both resolve through `AppPaths` (paths.py) — the asymmetry is real but bounded.
- Naming: `get_file_name` (`csv_service.py:237-240`) uses `strftime("%Y%m%d-%H%M%S")` — **second granularity**; same-second re-runs collide and overwrite (no `check_done` guard). The doc claim `data-flow.md:178` ("never overwrite") is false.
- Crash-safety: `df.write_csv` writes straight to the final path (`csv_service.py:356-361`) — no temp+rename. `export_path.mkdir(parents=True, exist_ok=True)` (`csv_service.py:306`).
- Chunking: `file_index` starts at 1 per run; `max_rows` defaults to `10000` (`csv_service.py:312`).
- **Evidence:** both export destinations & their resolved paths; one induced same-second overwrite; one mid-write crash and the truncated file left behind.

### Block 6 — Failure mode, crash-state preservation & observability
**Angle:** Audit teardown on both the happy and failure paths.
- DBWorker context manager: `__exit__` (`db_service.py:786-788`) always calls `self.close()` — no `exc_type` guard. `close()` (`db_service.py:769-781`) checkpoints WAL, closes both connections, then `_delete_base_files` (`db_service.py:762-767`) deletes `db_file.name + "*"` unconditionally. Diagnostic DB is unrecoverable on crash by design (`architecture.md:136-137`, `data-flow.md:36`).
- `ReportResult` (`models.py:170-195`) has **no error/status field**; a failed run raises, a successful-but-empty run returns `rows_exported == 0` indistinguishably from a full success. Since `process_data` returns this object (`app.py:138`), gap #8 is reachable, not hypothetical.
- Observability: run-path `print()` in Russian/English mix (`app_service.py:113-116`, `257-261`, `272-276`), plus `get_table_sample` `print()` (`csv_service.py:308`), and `progress_bar`'s `print()` (`utils.py:34-41`), vs. the `dictConfig`'d logging configured at `app_service.py:337`. Open-questions D-5 / OQ-10.
- **Evidence:** the `__exit__`→`close`→`_delete_base_files` path; one induced mid-run exception and the artefacts that remain (partial CSV only); the `ReportResult` field set.

> Note — this set drops the async/API framing but keeps the **pipeline** concept, which is the phase's raison d'être. It adds the crash-state-preservation, same-second-overwrite, and doc-vs-code-gap concerns that the original (async, retry-reliant) framing missed entirely.

## 2. Severity Taxonomy (re-based, synchronous)

Retain the **principle**: "grade by effect and blast radius; rate what is true now; an empty band is valid." Retain the four bands and labels verbatim. Replace every example with one anchored to the synchronous pipeline.

- **CRITICAL** — the run reports success while producing no/misleading output, or a crash destroys diagnostic state with no record.
  - *Concrete today:* `ReportResult` carries no `error`/`status` field (`models.py:170-195`), and `process_data` returns it (`app.py:138`) — so a caller reading `rows_exported == 0` cannot distinguish an empty run from a full one, and `export_sql_to_csv` returns `0` for an empty table without distinguishing "no input" from "no matches."
  - *Concrete today:* the diagnostic SQLite DB is **always deleted on exit** (`db_service.py:786-788, 762-781`), so a mid-run crash leaves no DB artefact for post-mortem — only a partial CSV. (Documented as intended in `architecture.md:136-137`; the band is a regression guard against the design intent ever *not* being honored.)
  - **Assessment:** both are *latent* today (no caller currently mis-reads the result in the test suite), so CRITICAL is **empty unless a regression guard is filed** — keep the band but mark it a guard, not a live defect.

- **HIGH** — silent data loss or a wrong target.
  - *Concrete today:* same-second re-run **overwrites** prior output (`csv_service.py:239`, `%Y%m%d-%H%M%S`, no collision guard) — two runs started within one second destroy the first run's files.
  - *Concrete today:* a partial CSV written mid-crash is read as complete (`csv_service.py:358-361`, non-atomic).
  - *Concrete today:* rules applied to the wrong table (`target_table` fallback when `index_column` is unset — verify in Block 3).

- **MEDIUM** — correctness gaps with bounded blast radius or observability debt.
  - *Concrete today:* the `null_data`/`clean_data` export-path asymmetry (`app_service.py:249` vs `:267`).
  - *Concrete today:* `build_non_mapped`'s predicate uses bitwise `|` on TEXT (`db_service.py:754`) — if proven always-false, the `null_data` dictionary-feedback channel is silently empty (a MEDIUM→HIGH escalation pending evidence in Block 2's provoke).
  - *Concrete today:* mixed Russian/English `print()` on the run path (`app_service.py:113-116, 257-261, 272-276`), violating the English-only house rule (`project.md` rule 1) — open-questions D-5/OQ-10.
  - *Concrete today:* `print()` progress not captured by the `dictConfig`'d logging (`utils.py:34-41`).

- **LOW** — drift and reproducibility, no runtime consequence.
  - *Concrete today:* doc/comment drift (`data-flow.md:165,178`; `architecture.md:218`) contradicting the code (gap #8 reachability, same-second overwrite).
  - *Concrete today:* non-reproducible second-granular filename (`csv_service.py:239`) — a reproducibility nit distinct from the HIGH data-loss consequence.
  - *Concrete today:* stale `__main__` dev guard hardcoding `data\mvideo` (`app_service.py:341-342`).
  - **Stale auditor item (do not carry):** "the empty top-level `__init__.py`" — `architecture.md:165` was corrected in Phase 01 §4.3; the file now exposes `__version__` but **no re-exports** (the real D-8 #4 item). Re-word to "no top-level re-exports (`src/mko_data_cleaner/__init__.py`)" if retained.

## 3. Report Output Settings (keep — with justification)

- **Findings path** `.ai/audit/04-data-pipeline/findings.md` — **KEEP.** The Phase 04 concept (pipeline) survives re-basing; the directory name is intact. (Cross-check the canonical layout — see §4.6-style note below; the working tree uses `.ai/reports/`, not `.ai/report/`, so confirm the singular-vs-plural finding path before finalizing, mirroring Phase 01 §4.6.)
- **Finding-ID prefix `PIPE-`** — **KEEP.** The subject is still the pipeline (import → match → rule → export). No new prefix is needed.
- **`problems-only: true`** — **KEEP.** All eight candidate gaps (§5) are code-verified, not hypothetical; dropping `problems-only` would dilute findings with passing checks. The phase is not an empty finding set.
- **Template / ≤100-line appends / empty-state wording** — unchanged.
- **`code_ref`** (template front matter) — populate with the git short SHA this report describes; write once, never per finding.

## 4. Scope Notes — Cross-Phase Citations

The current scope note (Phase 04 file lines 25–29) reads:

> *"Owned here: period resolution, task generation, worker loop ordering, data extraction, CSV export, and error serialisation. Owned elsewhere: the Semaphore and Queue concurrency (03); the retry and backoff contract (09); config defaults merge (02); performance ceilings (13)."*

All four "owned elsewhere" citations require correction. "Owned here" must also be re-based (drop period resolution/task generation/worker loop/error serialisation; they describe the deleted async model).

| Citation | Action | Rationale (code/doc evidence) |
|---|---|---|
| **(03) Semaphore/Queue concurrency** | **DELETE** | Phase 03's own phase file is **gone** from `.kilo/commands/audit/phases/` (only `phase-03-auditor-report.md` remains); the Phase 03 auditor report verdicts **DELETE** ("Async Concurrency & Worker Safety" — zero async primitives in `src/`). No concurrency layer exists (`grep` for `Semaphore`/`Queue`/`asyncio` in `src/mko_data_cleaner` is empty — verified). |
| **(09) retry/backoff contract** | **DELETE** | Phase 04 has no network and no retries (`architecture.md:37-40`: "External systems: none"). Phase 09 (`09-audit-external-api.md`) is itself async-framed (`asyncio.to_thread`, JSON credentials) and has no subject here — the auditor report §1 confirms Phase 09 has no subject. Removing the (09) exclusion from *Phase 04* is correct regardless of Phase 09's own state. |
| **(02) config defaults merge** | **REWORD** | Phase 02 has been re-based to the synchronous model (`02-audit-config.md` opening: "synchronous, single-shot, local-only … no network, no credentials"). The "defaults merge" premise is dead code: `merge_dicts` appears only in `utils.py:152-173` (definition + self-recursion) and is **never called** from `src/mko_data_cleaner/`. Re-word to "config values (02)" — the live settings that drive the pipeline (`app_config.yaml` → `DataSettings` at `models.py:158-164`; `data_file_settings`, `dict_file_settings`, `read_settings.from_csv`, `export_settings.to_csv`) are genuinely owned elsewhere. |
| **(13) performance ceilings** | **KEEP, reword** | Phase 13 is async-framed ("async data-fetch pipeline," `13-audit-performance.md` lines 13–21) and must be re-based separately; **but** the synchronous cost centres are real and code-verified and belong under Phase 13's remit, not Phase 04's. Re-word to "performance cost centres (13)". |

**Recommended rewrite of "Owned here / Owned elsewhere":**

> Owned here: run input scope & boundary detection, rule-block generation & application ordering, pipeline stage ordering & data-flow correctness, DataFrame round-trip fidelity & empty-result handling, export path resolution / naming / crash-safety, and failure-mode / crash-state / observability. Other phases own: settings values (02); SQLite/Polars/FTS performance cost centres (13).

## 5. Gap Analysis — The Eight Candidate Findings

All eight are **code-verified** and genuine. None are low-value noise; each names a concrete defect and a code site. The column below adds severity, the owning block, and the one doc-vs-code twist that raises the bar on two of them.

| # | Gap (auditor §5) | Severity | Owns block | Genuine? | Notes / refinements |
|---|---|---|---|---|---|
| 1 | Double DELETE in `_apply_delete` (`db_service.py:640,643`) | LOW | 2 | **Yes** | Already documented at `architecture.md:252` and `open-questions.md D-10` and `.ai/audit/problems/01-apply-delete-double-execution.md`. Surfacing it again in Phase 04 is redundant *unless* the Phase 04 finding cites that prior record and adds the `changes()`-based repro. Treat as a cross-reference, not a new finding. |
| 2 | Unconditional DB deletion on every exit incl. errors (`db_service.py:786-788, 762-781`) | MEDIUM | 6 | **Yes** | Real crash-state loss. But it is **intended design** (`architecture.md:136-137`; `data-flow.md:36`: "the database is deleted at the end"). File as a *design-intent gap* with a remediation path (e.g. skip delete, or checkpoint on a crash-only DB), not a regression. |
| 3 | `null_data`/`clean_data` export-path asymmetry (`app_service.py:249,267`) | LOW | 5 | **Yes** | Bounded: both paths are deterministic under `AppPaths`. Low blast radius; file as a reproducibility/clarity finding, MEDIUM at most if a consumer is shown to depend on the root location. |
| 4 | Same-second filename collision → overwrite (`csv_service.py:239`) | HIGH | 5 | **Yes** | **Raised by doc-vs-code gap:** `data-flow.md:178` asserts "repeated runs never overwrite previous outputs," which is false at second granularity. Real data-loss class; file as HIGH with the doc contradiction as corroborating evidence. |
| 5 | Non-atomic CSV export (`csv_service.py:358-361`) | MEDIUM | 5 | **Yes** | Temp+rename remediation. Genuine. |
| 6 | Bitwise `\|` in `build_non_mapped` predicate (`db_service.py:754`) | MEDIUM → **HIGH if dead** | 2 | **Yes** | **Raised by doc-vs-code gap:** `data-flow.md:87` documents the intended semantics ("where *any* extra column is NULL"), but `WHERE {a | b} IS NULL` on TEXT yields "all columns NULL" (SQLite coerces TEXT→0 for `|`), so the channel is likely silent. The block must prove `non_mapped` row count > 0 for a report with a partially-NULL extra column; if it is 0, escalate to HIGH (silent dictionary-feedback failure). |
| 7 | Mixed Russian/English `print()` on the run path (`app_service.py:113-116,257-261,272-276`; `csv_service.py:308`; `utils.py:34`) | MEDIUM | 6 | **Yes** | Already tracked as D-5 / OQ-10 in `open-questions.md`. Phase 04 Block 6 should reference it rather than re-file as a fresh defect; the observability angle (print not captured by `dictConfig`) is Phase 04-owned. Minor citation note: `csv_service.py:301` (cited once) is `logger.debug`, not `print`; the `print` is at `:308` and via `progress_bar` at `:350-354`. |
| 8 | `ReportResult` has no error field (`models.py:170-195`) | HIGH | 6 | **Yes** | **Raised by doc-vs-code gap:** `app.py:138` returns `ReportResult` (not `None` as `data-flow.md:165`/`architecture.md:218` claim), so the empty-report-is-silent defect is *reachable* by a pipeline caller. File as HIGH with the stale-docs discrepancy as corroboration: the success contract is silently widened to cover "zero rows exported." |

**Net:** 8/8 genuine. Two (#4, #8) are materially tighter because of doc-vs-code contradictions. One (#1) is already recorded elsewhere and should be cross-referenced. One stale auditor example ("empty `__init__.py`") is corrected in §2.

## 6. Edit Delta — Precise Rewrite Instructions

All references are to the **current** `.kilo/commands/audit/phases/04-audit-data-pipeline.md` (148 lines). The file's **front matter** (`name: 04-data-pipeline`, `status: draft`, `validated: no`, `executor: auditor`, `problems-only: true`) is consistent with the re-based Phase 02 and should be **kept as-is** — the front matter needs no change; only the H1 title and body sections are rewritten. Specifically: keep `problems-only: true`; keep `executor: auditor`; no front-matter change is warranted (this is an auditor-authored spec, not a doc-specialist-authored one).

| Lines | Replace with | Source § |
|---|---|---|
| 1–9 (front matter + H1) | Keep front matter. Rewrite H1 to `# Phase 04 — Data Pipeline & Report Lifecycle` (no "async" framing). | §1 |
| 11–30 (Purpose + Scope) | §1 block set intro + §4 rebased "Owned here / Owned elsewhere". Drop period resolution, task generation, worker loop, error serialisation as owned-here; add the six re-based blocks. Remove (03)/(09); reword (02); reword (13). | §1 + §4 |
| 33 (Audit Blocks intro) | Keep verbatim (independent blocks, evidence-class language). | — (unchanged) |
| 35–109 (Blocks 1–6, old) | **Replace** with the six re-based blocks in §1. Rename: "Period resolution"→"Run input scope"; "Task generation via Strategy"→"Rule-block generation & application ordering" (inject double-DELETE + D-2); "Worker loop: send→wait→extract"→"Pipeline stage ordering & data-flow correctness" (add `target_table` fallback check); "API result→DataFrame"→"DataFrame round-trip fidelity & empty-result handling" (drop API/strategy/KeyError; anchor on `clean_names`/`schema_overrides`/`return 0`); "CSV export safety"→"Export path resolution, naming & crash-safety" (invert the defect: it is overwrite not silent-skip; add doc contradiction `data-flow.md:178`); "Error serialisation & task-to-yaml"→"Failure mode, crash-state preservation & observability" (drop `TVTask.to_yaml`; anchor on `__exit__`→`close`→`_delete_base_files`, `print()` mix, `ReportResult` no error field, D-5/OQ-10). | §1 blocks 1–6 |
| 111–134 (Severity Taxonomy) | **Replace examples only**; keep the principle, the four band labels, and "empty band is valid." Use §2's re-based examples. **Delete** the stale LOW example "empty top-level `__init__.py`" (corrected in Phase 01 §4.3) and replace with "no top-level re-exports (`__init__.py`)". **Delete** the obsolete `mode="x"` / `TVTask.to_yaml` / API-polling / period-clamping examples. | §2 |
| 136–148 (Report Output) | **Keep:** findings path `.ai/audit/04-data-pipeline/findings.md`; prefix `PIPE-`; incremental append ≤100 lines; `problems-only: true`; empty-state wording; template + findings-template citation. **Add:** the `code_ref` front-matter note (template §code_ref) and the singular-vs-plural findings-path confirmation caveat from §3. | §3 |

**Do not touch:** the `problems-only: true` front-matter flag; the "≤100-line passes" rule; the empty-state wording.

**Must surface in the rewritten file (not buried in problems notes):**
- The double-DELETE already has its own problem note (`01-apply-delete-double-execution.md`) and `architecture.md:252` — the rewritten Block 2 should cite it, not re-derive it.
- The `data-flow.md:178` "never overwrite" and `data-flow.md:165`/`architecture.md:218` "returns None" contradictions are **documentation defects** that belong in the Phase 04 findings (doc-vs-code drift), not only in the open-questions register.

---

*End of recommendation. This document is evidence-grounded: every structural recommendation traces to a verified current-source fact (the 14-step `run_report` chain at `app_service.py:110-290`; the double DELETE at `db_service.py:630-643`; the unconditional `__exit__→close→_delete_base_files` at `db_service.py:762-788`; the bitwise-predicate at `db_service.py:754`; the `%Y%m%d-%H%M%S` timestamp at `csv_service.py:239`; the non-atomic write at `csv_service.py:356-361`; the path asymmetry at `app_service.py:249,267`; the no-error `ReportResult` at `models.py:170-195`; the dead `merge_dicts` at `utils.py:152-173`; and the doc-vs-code contradictions in `data-flow.md`/`:165,178` and `architecture.md:218`). The two corrections the auditors' own report understates are gap #8's reachability (`app.py:138` returns `ReportResult`) and gap #4's data-loss lift (`data-flow.md:178` is contradicted). No source files were modified by this analysis.