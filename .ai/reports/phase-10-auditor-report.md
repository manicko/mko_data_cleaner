---
phase: "10"
phase_name: "Code Quality"
phase_file: ".kilo/commands/audit/phases/10-audit-code-quality.md"
report_status: "draft"
scope: >-
  re-basing assessment of Phase 10 against a synchronous, single-shot,
  local-only CLI tool + library (no event loop, no API, no network, no
  credentials). Code quality convention blocks are framework-agnostic and
  require only citation cleanup + minor rewording.
code_ref: "384044a"
recommendation: REBASE
---

# Audit Report — Phase 10 Code Quality (Rework Assessment)

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **synchronous, single-shot, local-only** CLI tool and
Python library that post-processes Mediascope TV advertising exports received as
**files on disk**. There is no outbound API, no credentials, no network, no
event loop, and no async/sync bridge anywhere in the architecture. The runtime
is a straight synchronous call chain:

```
app.py → AppService.run_report → CSVWorker → DBWorker / MappingDict
```

Code-verified facts (`src/mko_data_cleaner/`, `docs/00-overview/`, `pyproject.toml`):

- **Zero async primitives.** A repo-wide grep for `asyncio`, `async def`,
  `asyncio.run`, `Semaphore`, `Queue`, `requests`, `httpx`, `aiohttp`,
  `token`, `credential`, `api_key` across `src/mko_data_cleaner` returns zero
  code matches. The only `https://` hit is a SQLite documentation URL comment
  (`db_service.py:435`).
- **No network / no credentials.** `architecture.md:37-40` states "External
  systems: none." `pyproject.toml` declares no HTTP/async client dependency.
  No `.env*` files exist anywhere in the repo.
- **One real import-time side effect.** `sys.stdout.reconfigure(line_buffering=
  True)` sits at module level in `app.py:23-24`; `core/*` modules do **not**
  reconfigure stdout.
- **Lazy service factory.** `get_app_service()` (`app_service.py:325-338`)
  constructs `AppService` only on call, after the `_require_settings` gate
  passes; `dictConfig` runs inside the factory, never at import.

**Conclusion for Phase 10:** The nine code-quality blocks are **framework-
agnostic** — each is a lens on convention, consistency, and enforcement inside
the code itself, regardless of whether the runtime is async or sync. Unlike
Phase 03 (async concurrency) or Phase 09 (external API), Phase 10 has **six
fully applicable blocks** (1, 2, 3, 4, 5, 6, 7, 8) and one that needs a
one-word rewording (Block 9). The phase is **REBASE, not DELETE**.

---

## 2. Per-Block Assessment (1–9)

Each block is assessed for **Fit** (does the concept apply to the synchronous
codebase), **Outdated** (async/API/credential references that need rewording),
and **Replacement** (what changes are needed — reword, add evidence anchors,
remove async framing).

### Summary table

| Block | Current title | Fit | Outdated (async/API references) | Replacement needed |
|---|---|---|---|---|
| 1 | Fixed values: one named home per value | **Full** | None | No change — add evidence anchors |
| 2 | The constant surface as a maintained artifact | **Full** | None | No change — add evidence anchors |
| 3 | Boundary validation coverage | **Full** | None | No change — add evidence anchors |
| 4 | Type width and the sanctioned-suppression surface | **Full** | None | No change — add evidence anchors |
| 5 | Responsibility inside a unit | **Full** | None | No change — add evidence anchors |
| 6 | One rule, one home | **Full** | None | No change — add evidence anchors |
| 7 | Convention compliance: the cheap high-volume rules | **Full** | None | No change — Block 7 is fully aligned |
| 8 | Declared contracts and their enforcement point | **Full** | None | No change — works as-is for sync |
| 9 | Template and config drift | **Partial** | "what the API receives" (L106) — no API exists | Reword to "what the pipeline receives" |

### Block 1 — Fixed values: one named home per value
- **Fit: Full.** The concept (every fixed value the system branches on has
  exactly one named home; raw literals at a second site are the finding) is
  universal. The codebase has `StrEnum` vocabularies in `core/models.py`
  (`ActionType`, `MatchType`, `MappingColumns`, `DataFileExtension`) and raw-
  literal sites in `core/db_service.py`. The block is fully applicable.
- **Outdated:** None. No async/API framing.
- **Replacement:** No rewording needed. Add evidence anchors for candidate
  findings (see §4 Candidate Findings): `AVAILABLE_ACTIONS` dict
  (`db_service.py:39-43`) duplicates `ActionType` enum (`models.py:106-109`)
  with zero call sites; `ALLOWED_PATTERN` (`utils.py:15`) duplicates the
  `NameConstrained` regex (`models.py:29-36`); `DataFileExtension` defines
  `default="csv"` and `csv="csv"` as duplicate values (`models.py:72-73`).

### Block 2 — The constant surface as a maintained artifact
- **Fit: Full.** The concept (constants as a maintained artifact, not an
  ad-hoc registry) applies. The codebase has `VALID_COLUMN_DTYPES`,
  `DATA_TO_SQL_PARAMS`, `ALLOWED_PATTERN`, `AVAILABLE_ACTIONS` — all
  module-level constants. The block's questions ("is numbering/order
  documented?", "is membership derived outside the vocabulary?", "is a value
  table constructed inside the function that consumes it?") are all valid
  for this codebase.
- **Outdated:** None.
- **Replacement:** No rewording needed. Add evidence anchors.

### Block 3 — Boundary validation coverage
- **Fit: Full.** The concept (which input boundaries validate, which do not;
  transport → parse → validation model → persistence) applies. In this sync
  codebase the boundaries are: YAML file → `yaml_to_dict` → Pydantic model
  (`DataSettings`/`LoggingSettings`) → `AppService` → SQLite. The
  `extra` policy inconsistency (`WorkingPaths` `extra="forbid"` at
  `models.py:61` vs six siblings `extra="allow"` `models.py:78,85,94,135,143`
  vs roots `DataSettings`/`LoggingSettings` with no `model_config` →
  `extra="ignore"` `models.py:51,158`) is a direct instance of this block's
  "a model that sets a permissive unknown-key policy" finding.
- **Outdated:** None. The phrase "what reaches persistence" is valid (SQLite
  is the persistence layer).
- **Replacement:** No rewording needed. Add evidence anchors for the `extra`
  policy inconsistency (Phase 05 gap #7).

### Block 4 — Type width and the sanctioned-suppression surface
- **Fit: Full.** The concept (what the type checker enforces, disabled
  diagnostic classes, widened annotations classified as load-bearing or
  accidental) applies to any Python project with `basedpyright`/`pyright`.
  This codebase uses `basedpyright` (per `.kilo/commands` documentation)
  with `pyproject.toml` type-checking config.
- **Outdated:** None.
- **Replacement:** No rewording needed. Add evidence anchors for candidate
  findings: `DataFile.index_column: str = None`, `date_column: str = None`
  (`models.py:80-81`) — annotation `str` with `None` default;
  `PolarsReadCSV.infer_schema_length: bool = False` (`models.py:102`) —
  boolean where Polars expects `int | None` (Phase 05 gap #2).

### Block 5 — Responsibility inside a unit
- **Fit: Full.** The concept (how many distinct responsibilities one view/
  handler/service/helper performs; whether extracting one would change what
  tests can reach) applies. `AppService.run_report` (`app_service.py:110-290`)
  is a 180-line method performing: orchestration, CSV ingestion, dictionary
  processing, DB schema setup, data import, rule-block matching, non-mapped
  extraction, index/data synchronization, and final export — multiple
  responsibilities in one unit.
- **Outdated:** None.
- **Replacement:** No rewording needed. Add evidence anchors.

### Block 6 — One rule, one home
- **Fit: Full.** The concept (for each rule more than one component must obey,
  exactly one implementation exists) applies. Candidates: `AVAILABLE_ACTIONS`
  (`db_service.py:39-43`) vs `ActionType` (`models.py:106-109`); `ALLOWED_PATTERN`
  (`utils.py:15`) vs `NameConstrained` (`models.py:29-36`);
  `_apply_delete` executing its DELETE twice (`db_service.py:630-643`).
- **Outdated:** None.
- **Replacement:** No rewording needed. Add evidence anchors.

### Block 7 — Convention compliance: the cheap high-volume rules
- **Fit: Full.** The concept (standard-output writes on a production path,
  comment/docstring/error language, naming, import ordering, import-time side
  effects) applies directly. **Block 7 is fully aligned** with the actual
  codebase:
  - `sys.stdout.reconfigure(line_buffering=True)` at `app.py:23-24` — IS an
    import-time side effect (the block calls it out).
  - `progress_bar` in `utils.py:18-41` uses bare `print()` on a production
    path (called from `app_service.py:219`).
  - `_apply_delete` double execution (`db_service.py:630-643`) — a "second
    site" duplicate.
  - `merge_dicts` (`utils.py:152-173`) — present, zero call sites (the
    block's "present, reachable, referenced by nothing" question).
- **Outdated:** None. The block's mention of "import-time side effect"
  (line 89) is **directly relevant** — `app.py:23-24` is the one real
  import-time side effect in the codebase.
- **Replacement:** No rewording needed. This block needs no changes; the
  existing evidence guidance is acceptable. Block 7 is the strongest fit
  among all nine.

### Block 8 — Declared contracts and their enforcement point
- **Fit: Full.** The concept (every property the code or docs asserts;
  whether anything checks it; whether anything reaches it; a comment that
  describes a condition differently from the executing path) applies to any
  codebase. The codebase has: `yaml_to_dict` returning `{}` on error with
  a docstring claiming "or an empty dict if the file does not exist or is
  invalid" but no caller checking the cause; `AVAILABLE_ACTIONS` declared
  but never referenced (a "declared vocabulary offered in a surface with
  no enforcing path"); `_apply_delete` running its DELETE twice (the
  "comment that describes a condition differently from the executing path"
  anti-pattern is alive if debug logging is treated as a contract).
- **Outdated:** None. No async/API framing.
- **Replacement:** No rewording needed. Add evidence anchors.

### Block 9 — Template and config drift
- **Fit: Partial.** The concept (template↔Pydantic-model drift detection;
  whether a check exists and runs repeatably; whether drift is caught) is
  **valid** — Phase 05 has exhaustively mapped the real model inventory
  (`DataSettings`/`LoggingSettings`, `WorkingPaths`/`DataFile`/`DataDict`/
  `Database`/`PolarsReadCSV`/`PolarsWriteCSV`/`DictColumnsIndexes`). The
  *question* is correct.
- **Outdated: "what the API receives" (line 106).** The codebase has **no
  API**. Data arrives as files on disk and flows through a synchronous
  pipeline (CSV → Polars → SQLite → CSV). The phrase "what the API receives
  / what the API request" must be reworded to "what the pipeline receives /
  what the pipeline input." The rest of the block (template-to-model
  drift, idempotency, gate detection) is framework-agnostic and applies.
- **Replacement:** Reword all references to "API" in Block 9 to "pipeline":
  - Line 106: "affecting what the API receives" → "affecting what the
    pipeline receives"
  - Line 108: "a change to a config template or Pydantic model that affects
    what the API request" → "a change to a config template or Pydantic model
    that affects what the pipeline receives"
  - Line 109: "template key or model field that affects the API request" →
    "template key or model field that affects the pipeline input"

---

## 3. Purpose & Scope Boundary Assessment

### Purpose text (lines 13–15)
- **Assessment: Keep verbatim.** The purpose is framework-agnostic: "Audits
  whether the code holds to the conventions the project has adopted, and whether
  the properties the codebase and its documentation claim are the properties that
  actually hold." No async/network/API/credential references.
- **No changes required.**

### Scope boundary (line 17)
Current text:
> Scope boundary — other phases own: settings values and environment policy (02);
> transaction, lock, and pooler semantics (03); lifecycle correctness (05); the
> event-loop bridge mechanism (09); test adequacy (11); pipeline and container
> posture (12); measurable performance consequences (13); authorization (15).

| Citation | Phase exists? | Status of cited phase | Action | Rationale |
|---|---|---|---|---|
| (02) | Yes | Re-based to Configuration Management (sync) | **KEEP** | Settings values & env policy are a real, synchronous concern owned by Phase 02. |
| (03) | No | Deleted (Phase 03 auditor report §5) | **REMOVE** | "transaction, lock, and pooler semantics" was async-framed (Semaphore/Queue/worker-pool). Phase 03 deleted. SQLite transaction/lock semantics belong to the pipeline/DB layer, owned by Phase 04. |
| (05) | Yes | Re-based to Config Template Validation (sync) | **KEEP** | Template validation is a real, synchronous concern. |
| (09) | No | Deleted (Phase 09 file already removed; researcher rec §2) | **REMOVE** | "the event-loop bridge mechanism" was async-framed (`asyncio.to_thread`). Phase 09 deleted; no bridge exists. |
| (11) | Yes | Exists (sync) | **KEEP** | Test adequacy is a real concern for any codebase. |
| (12) | Yes | Async-framed, to be re-based | **KEEP (reword)** | "pipeline and container posture" is a real concern (observability, packaging). Phase 12 is async-framed but still exists; the concept survives re-basing. |
| (13) | Yes | Async-framed, to be re-based | **KEEP (reword)** | "measurable performance consequences" is a real concern (Polars I/O, SQLite bulk writes). Phase 13 is async-framed but still exists; the concept survives re-basing. |
| (15) | Yes (but vestigial) | Async/API/credential-framed, to be deleted | **REMOVE** | "authorization (15)" — there are no credentials, no network, no auth in this codebase. Phase 15 ("Authorization & Credential Lifecycle") is a deletion candidate by the same precedent as Phase 03/09. Its scope citation to (15) must be removed from Phase 10. |

**Resulting scope boundary (recommended edit to line 17):**
> Scope boundary — other phases own: settings values and environment policy (02);
> lifecycle correctness (05); test adequacy (11); pipeline and container posture (12);
> measurable performance consequences (13).

(Remove "transaction, lock, and pooler semantics (03);" and "the event-loop bridge
mechanism (09);" and "authorization (15).")

### Purpose line 18 ("authorization (15)")
The task brief notes "Purpose line 18 mentions 'authorization (15)'." In the actual
file, "authorization (15)" appears on **line 17** (the scope-boundary sentence),
not line 18 (which begins "This phase owns convention and consistency inside the
code"). The mention is the same one addressed in the table above — **remove it**
because Phase 15 is a deletion candidate with no synchronous subject.

### Purpose lines 18–21 (the rest of the purpose paragraph)
- "This phase owns convention and consistency inside the code" — **valid**, keep.
- "Two live implementations of one derived quantity, and an aggregate definition no
  caller reaches, are this phase's HIGH and LOW bands" — **valid**, keep. Block 7
  confirms this: `AVAILABLE_ACTIONS` duplicates `ActionType`; two live definitions.
- "17 establishes the arithmetic showing whether they agree" (line 20) — **valid**,
  keep: Phase 17 (Arithmetic) exists and its purpose (line 20) explicitly says "Where
  two implementations of one derived quantity exist, Phase 10 records the duplication
  and this phase establishes whether they agree." This cross-phase handoff is correct.

---

## 4. Candidate Findings (with code refs)

These are findings that Phase 10, when executed, should surface. They are grouped
by the block that catches them. **Not all belong to Phase 10** — where a root cause
is owned by another phase, the finding is flagged for cross-filing (Phase 10 records
the duplication/dead-code signal; the owning phase fixes the root cause).

### Block 1 / Block 6 findings

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F1. `AVAILABLE_ACTIONS` dict duplicates `ActionType` enum with zero call sites.** `db_service.py:39-43` defines `{"DELETE": "d", "REPLACE": "r", "ADD": "a"}`; `models.py:106-109` defines `ActionType` StrEnum with the same three values. The dict is never read (grep: zero hits in `src/` or `tests/`). Two definitions of one rule, one dead. | `db_service.py:39-43` vs `models.py:106-109` | MEDIUM | Phase 10 (duplication); Phase 02 (dead code) |
| **F2. `ALLOWED_PATTERN` regex duplicates `NameConstrained` regex.** `utils.py:15` defines `ALLOWED_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$"`; `models.py:29-36` defines `NameConstrained` with the identical pattern. Two definitions of one validation rule. | `utils.py:15` vs `models.py:29-36` | LOW | Phase 10 (one rule, one home) |
| **F3. `DataFileExtension` has duplicate enum values.** `models.py:72-73`: `default = "csv"` and `csv = "csv"` — `csv` is an alias of `default`. Phase 05 gap #11. | `models.py:72-73` | LOW | Phase 10 (fixed-value enum, Block 1) |

### Block 3 / Block 8 findings

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F4. `WorkingPaths` uses `extra="forbid"` while six sibling models use `extra="allow"` and two roots use `extra="ignore"`.** Inconsistent `extra` policy across the validation boundary with no recorded reason for each departure. Phase 05 gap #7. | `models.py:61` (forbid) vs `models.py:78,85,94,135,143` (allow) vs `models.py:51,158` (ignore) | MEDIUM | Phase 10 Block 3 (boundary validation coverage) |
| **F5. `yaml_to_dict` returns `{}` on `FileNotFoundError` or `YAMLError`, swallowing config errors.** The docstring claims this is intentional, but no caller checks the cause before constructing `DataSettings(**{})` → opaque "field required" `ConfigError`. Shipped tests assert the mask (`tests/test_app.py:207-215`). | `utils.py:131-149` | HIGH (root cause) | **Phase 02** (root cause); Phase 10 records the contract enforcement gap |

### Block 4 findings

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F6. `DataFile.index_column` and `date_column` annotated `str` but defaulted to `None`.** `models.py:80-81`: `index_column: str = None`, `date_column: str = None`. Phase 05 gap #1. Type checkers flag this; a `None` also flows into `DBWorker` target-table logic (`db_service.py:76-79`). | `models.py:80-81` | LOW | Phase 10 (type width, Block 4) |
| **F7. `PolarsReadCSV.infer_schema_length` typed `bool` but Polars expects `int \| None`.** `models.py:102`: `infer_schema_length: bool = False`. `False` → `0` → forces all columns to `String`. Phase 05 gap #2. | `models.py:102` | MEDIUM | Phase 05 (New 3); Phase 10 notes the type-width gap |

### Block 5 findings

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F8. `__main__` block in `app_service.py` hardcodes a Windows-specific data path.** `app_service.py:341-342`: `get_app_service().run_report(r"data\mvideo")`. A dev-only execution block in an internal orchestrator module that hardcodes a data path and calls `run_report` (which needs settings — will fail without `_require_settings` passing). Documented as LOW hygiene in Phase 01 auditor report §2 Block 5 note. | `app_service.py:341-342` | LOW | Phase 10 (responsibility inside a unit) / Phase 01 (entry-layer discipline) |

### Block 6 findings

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F9. `_apply_delete` executes the identical DELETE statement twice.** `db_service.py:630-643`: the same `sql` is passed to `perform_query` on lines 640 and 643. The second execution is a no-op (rows already deleted) but re-evaluates the subquery. Already documented in `.ai/audit/problems/01-apply-delete-double-execution.md`. | `db_service.py:630-643` | LOW | Phase 10 (one rule, one home) |

### Block 7 findings

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F10. Bare `print()` on the production path.** `progress_bar` (`utils.py:34,41`) uses `print()` instead of the configured logger; it is called from the production path `AppService.run_report` (`app_service.py:219`). `get_table_sample` (`db_service.py:308-309`) also uses `print()`. This bypasses the `dictConfig`-configured logging stack. | `utils.py:34,41` (called from `app_service.py:219`); `db_service.py:308-309` | MEDIUM | Phase 10 (convention: stdout writes on production path) |
| **F11. Mixed Russian/English output strings.** `app_service.py:113` prints `Обработка стартовала` (Russian); `app_service.py:257` prints `Synchronizing the index table...` (English); `app_service.py:272` prints `Обработка завершена` (Russian). The `app.py` module docstring (`app.py:2`) and all Typer CLI help strings are Russian. The project rules (English-only, `rules/project.md` §1) require all logs, error messages, and docstrings in clear English. | `app_service.py:113,257,272`; `app.py:2,29,37,49,71,104` | MEDIUM | Phase 10 (convention: language of comments/log/error text) |
| **F12. Dead code: `merge_dicts` and `AVAILABLE_ACTIONS` and `get_table_sample`.** `merge_dicts` (`utils.py:152-173`): zero call sites in `src/` or `tests/`. `AVAILABLE_ACTIONS` (`db_service.py:39-43`): zero call sites (see F1). `get_table_sample` (`db_service.py:300-309`): zero call sites; also uses `print()`. All present, reachable, referenced by nothing (Block 7's exact question). | `utils.py:152-173`; `db_service.py:39-43`; `db_service.py:300-309` | LOW | Phase 02 (dead code sweep) / Phase 10 (records the unreferenced definitions) |

### Block 9 findings

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F13. No drift detection gate exists for template↔model changes.** When a field is added to a YAML template or a Pydantic model, nothing automatically checks that the counterpart was updated. Phase 05 New 1–6 and the 13-gap table document 5 concrete drift cases (gap #2 `infer_schema_length` type mismatch, gap #4 `include_bom` undeclared, gap #7 `extra` inconsistency, gap #12 missing `model_config`, gap #11 `DataFileExtension` alias). No CI gate detects these. | `models.py`, `app_config.yaml` | HIGH | Phase 05 (template/model conformance) / Phase 10 (records the missing enforcement point) |

---

## 5. Severity Taxonomy Assessment

**Verdict: Keep as-is.**

Phase 10's severity taxonomy (lines 113–125) opens with the **sound principle**:
"Grade by effect and blast radius, not by mechanism name; rate what is true now,
not the worst consequence if triggered. The bands are effect classes, not a closed
list of mechanisms."

**Every illustrative example is framework-agnostic** and applies directly to the
synchronous, file→SQLite pipeline:

- **CRITICAL:** "a stored value that no rule ever constrained" — applies (e.g., `index_column`/`date_column` `None` flowing into DB logic, gap #1). "a field default that overwrites stored content when the key is absent" — applies (SQLite `UPDATE` in `_sync_tables`, `db_service.py:388-426`). "two implementations of one rule whose copies already disagree on a value the system acts on" — applies (`AVAILABLE_ACTIONS` vs `ActionType`, though they agree today).
- **HIGH:** "silent today, and the next change is guaranteed to be made wrongly in the same place: a value with two homes" — applies (`AVAILABLE_ACTIONS`/`ActionType`, `ALLOWED_PATTERN`/`NameConstrained`). "schema drift no gate would notice" — applies (F13). "a declared invariant nothing enforces" — applies (docstring claims in `yaml_to_dict`).
- **MEDIUM:** "an unvalidated boundary on a non-critical path" — applies (`get_table_sample`). "a widened type on a value nothing branches on" — applies (`infer_schema_length: bool`). "a constant surface whose conventions are undocumented" — applies.
- **LOW:** "a declared rule no caller reaches" — applies (`AVAILABLE_ACTIONS`, `get_table_sample`). "a private name in a public export list" — applies (`app.py:157-164` `__all__` over-exports internals). "an unreferenced definition nobody has explained" — applies (`merge_dicts`, `get_table_sample`).

**No examples reference async, API, event-loop, credentials, or retry.** The
taxonomy needs **no rewording** — it is valid for the synchronous codebase as-is.

---

## 6. Report Output Assessment

| Item | Current (Phase 10 §Report Output) | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/10-code-quality/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`). Directory does not yet exist — no prior findings to reconcile. |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template; front matter defines `severity_taxonomy` and `code_ref` fields. |
| ID prefix | `QLT-` | **Keep** — but **correction needed** | See §6.1 below. |
| problems-only | `true` (front matter L6 + §L134) | **Keep** | All candidate findings are code-verified defects, not passing checks. |
| per-pass lines | ≤100, incremental append (L133) | **Keep** | Template constraint. |
| Empty state | `No problems found in this phase.` (L134) | **Keep** | Verbatim. |
| Shipped-test-as-blocker | L135: "a shipped test asserting current behaviour is a remediation blocker" | **Keep** | Applies to F5 (`yaml_to_dict` `{}` fallback, `tests/test_app.py:207-215`). |

### 6.1 The `QLT-` prefix collision warning — DISPROVEN

Phase 10 line 131 states:
> Finding-ID prefix: `QLT-` — **already in use in shipped source from a prior
> cycle, with at least one identifier already resolving to two different
> findings; check for a collision before minting an ID and report it rather
> than creating a second namespace.**

**This claim is disproven by the working tree (code_ref `384044a`):**

- A repo-wide grep for `QLT-\d` returns **5 matches**, all in the **template**
  file `.ai/audit/templates/audit-findings.md` (lines 48, 49, 54, 72, 81) — these
  are **placeholder examples** in the generic template, using paths from a
  completely different project (`src/backend/apps/ads/models.py::Ad.transition_to`,
  `src/backend/apps/ads/services/submission.py::submit_ad`, `AUTZ-002`). They are
  **not** findings files or finding IDs in any findings document.
- A repo-wide grep for `QLT-` (without `\d`) returns **7 matches**: the 5
  template placeholders above plus the Phase 10 phase file itself (line 131,
  the warning) and a scratch copy in `.ai/tmp/` (not shipped).
- **No findings directory `.ai/audit/10-code-quality/` exists** (`Test-Path` →
  `False`; `glob .ai/audit/**` shows only `templates/` and `problems/`).
- **No `.ai/audit/10-code-quality/findings.md` file exists.**
- No shipped source file (`.py`, `.yaml`, `.txt`) contains `QLT-`.

**Conclusion:** There is no "prior cycle" `QLT-` usage in shipped source. The
template's `QLT-001`/`QLT-002` are generic placeholders that ship inside the
template scaffold itself (used by all phases as the starting doc) — they use
hardcoded example paths from a Django ads app, which is why they don't correspond
to real findings in *this* repo. The Phase 10 phase file's warning appears to
be **stale phantom evidence** — it may have referred to a prior audit cycle that
has since been cleared, or may have conflated the template placeholders with
actual findings.

**Recommendation:** Retain the `QLT-` prefix (it matches the template
convention and is unique among real findings — zero collisions found). **Remove
or correct the "already in use" warning** on Phase 10 line 131. The corrected
text should state that `QLT-` is the Phase 10 prefix per the template
convention and that no collision was found in the current tree.

---

## 7. Cross-Phase Citation Cleanup

### 7.1 Phases that Phase 10 cites (scope boundary, line 17)

| Cited phase | Current citation text | Status | Action |
|---|---|---|---|
| (02) | "settings values and environment policy" | Re-based, exists | **KEEP** |
| (03) | "transaction, lock, and pooler semantics" | Deleted (§5) | **REMOVE** |
| (05) | "lifecycle correctness" | Wait — see note below | **KEEP with note** |
| (09) | "the event-loop bridge mechanism" | Deleted (file removed) | **REMOVE** |
| (11) | "test adequacy" | Exists | **KEEP** |
| (12) | "pipeline and container posture" | Async-framed, to be re-based | **KEEP (reword when 12 re-based)** |
| (13) | "measurable performance consequences" | Async-framed, to be re-based | **KEEP (reword when 13 re-based)** |
| (15) | "authorization" | Async/API/credential-framed, to be deleted | **REMOVE** |

**Note on (05):** Phase 10 line 17 cites (05) as "lifecycle correctness." Phase 05's
actual purpose is "Config Template Validation" (re-based from async). The citation
text "lifecycle correctness" is slightly mismatched to Phase 05's current remit
(template↔Pydantic conformance), but the concept "lifecycle correctness" does not
belong to Phase 10 — it is genuinely owned elsewhere. Since Phase 05 has been
re-based and no longer owns "lifecycle correctness" (that concept is now Phase 01's
domain — "lazy service factory and bootstrap ordering"), the citation to (05) should
be **REWORD** to either (a) point to the real Phase 05 remit ("template validation"),
or (b) be removed entirely if Phase 10 doesn't need config-template validation as
a scope boundary. **Recommendation: reword to (01)** for import-time bootstrap +
config gate, and **remove the (05) citation** since "lifecycle correctness" is not
Phase 05's current scope.

**Recommended rework of line 17:**
> Scope boundary — other phases own: settings values and environment policy (02);
> import-time bootstrap and the settings gate (01); test adequacy (11); pipeline
> and container posture (12); measurable performance consequences (13).

### 7.2 Phases that cite (10) — who references Phase 10

| Phase | Line | Citation text | Phase status | Action on (10) citation |
|---|---|---|---|---|
| 02 | 22 | "enum discipline (10)" | Re-based (sync) | **KEEP** — valid; Phase 10 owns enum discipline |
| 05 | 31 | "fixed-value enum discipline (10)" | Re-based (sync) | **KEEP** — valid |
| 11 | 18 | "production-code convention defects and dead code (10)" | Exists (sync) | **KEEP** — valid |
| 12 | 25 | "code-quality discipline (10)" | Async-framed, to be re-based | **KEEP (valid target)**; verify relevance when 12 is re-based |
| 13 | 27 | "code-quality discipline (10)" | Async-framed, to be re-based | **KEEP (valid target)**; verify relevance when 13 is re-based |
| 16 | 27 | "fixed-value enum discipline (10)" | Async-framed, to be re-based | **KEEP (valid target)**; verify relevance when 16 is re-based |
| 17 | 27 | "code-quality detection of duplicated implementations (10)" | Exists (sync-ish, async-framed purpose) | **KEEP** — valid; Phase 10 Block 1/6 catches duplicates |

Phase 15 (which cites "enforcement (10)" at line 28) is a deletion candidate —
its citation to (10) is removed when the phase file is deleted. No action needed
in Phase 10.

**Note:** Phase 03 (which cited (10) at its deleted line 17) and Phase 09 (which
cited (10) in its deleted scope) are already removed from the tree — their (10)
citations are gone with the files.

---

## 8. Block 9 Rewording Plan (specific edits)

Target: `.kilo/commands/audit/phases/10-audit-code-quality.md`, Block 9 (lines 104–111).

| Line | Current text | Reworded text |
|---|---|---|
| 106 | "...every change to a config template or Pydantic model that affects **what the API receives**, whether the change is recorded..." | "...every change to a config template or Pydantic model that affects **what the pipeline receives**, whether the change is recorded..." |
| 107–108 | "...whether a check present in configuration is actually executed on every change..." | *no change needed* — valid |
| 109 | "...whether a template key or model field that affects **the API request** can be added or altered in one place without a corresponding update to its counterpart..." | "...whether a template key or model field that affects **the pipeline input** can be added or altered in one place without a corresponding update to its counterpart..." |
| 110 | "Evidence: the gate inventory — which drift checks exist, where they run, and what they cover..." | *no change needed* |

The rest of Block 9 (lines 107–108, 110–111) uses framework-agnostic language
("configuration," "Pydantic model," "template key," "model field") and needs
**no other changes**. The concept of template↔model drift detection is directly
applicable — Phase 05 has already mapped the real model inventory and documented
5 concrete drift gaps.

---

## 9. Purpose Rewording Plan (specific edits)

Target: line 17 (scope boundary). No changes to the Purpose paragraph (lines
13–15) — it is already framework-agnostic.

| Line | Current text | Reworded text |
|---|---|---|
| 17 | "...settings values and environment policy (02); transaction, lock, and pooler semantics (03); lifecycle correctness (05); the event-loop bridge mechanism (09); test adequacy (11); pipeline and container posture (12); measurable performance consequences (13); authorization (15)." | "...settings values and environment policy (02); import-time bootstrap and the settings gate (01); test adequacy (11); pipeline and container posture (12); measurable performance consequences (13)." |

Rationale:
- Remove (03) — Phase 03 deleted; SQLite transaction/lock semantics belong to
  Phase 04 (pipeline/DB layer), not Phase 10.
- Remove (09) — Phase 09 deleted; no event-loop bridge exists.
- Reword (05) "lifecycle correctness" → (01) "import-time bootstrap and the
  settings gate" — this is what Phase 01 actually owns now (lazy factory +
  `_require_settings` gate, per Phase 01 auditor report §2 Block 2/4).
- Remove (15) — Phase 15 is a deletion candidate (async/API/credential-framed,
  no subject in this codebase).

---

## 10. Conclusion

**Verdict: REBASE (do not delete, do not fully rework).**

Phase 10's nine code-quality blocks are **framework-agnostic** principles that
apply fully to the synchronous, single-shot, local-only codebase. Unlike Phase
03 (async concurrency) or Phase 09 (external API), Phase 10 requires **no block
deletion** — eight blocks are Fit: Full with zero outdated references, and one
block (Block 9) needs a single one-word rewording ("API" → "pipeline").

The required rework is **minor citation cleanup + specific rewording only**:

1. **Line 17 (scope boundary):** Remove citations to deleted phases (03, 09,
   15); reword (05) "lifecycle correctness" → (01) "import-time bootstrap and
   the settings gate"; keep (02), (11), (12), (13).
2. **Block 9 (lines 106, 109):** Reword "what the API receives" → "what the
   pipeline receives" and "the API request" → "the pipeline input."
3. **Line 131 (Report Output):** Remove or correct the `QLT-` "already in use"
   collision warning — no collision exists in the current tree (disproven by
   grep; only template placeholders exist, using a different project's paths).
4. **Severity taxonomy (lines 113–125):** No changes — all examples are
   framework-agnostic and apply to the synchronous codebase.

**Candidate findings: 13 findings** (F1–F13) are documented in §4 with full
code references, severity bands, and ownership attribution. They span all nine
blocks and should be filed under the `QLT-` prefix when Phase 10 is executed.
Several have root causes owned by other phases (F5 → Phase 02, F7 → Phase 05,
F8 → Phase 01/10) — Phase 10's role is to record the duplication/missing-
enforcement signal and cross-reference the owning phase, not double-fix.

**Effort: small** (≤4 line edits to the phase file; 13 findings to file in a
new `.ai/audit/10-code-quality/findings.md`). **Priority: recommended** — the
citations to deleted phases (03, 09, 15) in Phase 10's scope boundary are
actively misleading and should be corrected when Phase 10 is formally executed.
No source files are modified by this report.
