---
phase: "10"
phase_name: "Code Quality"
phase_file: ".kilo/commands/audit/phases/10-audit-code-quality.md"
source: ".ai/reports/phase-10-auditor-report.md"
report_status: "draft"
recommendation: REBASE
code_ref: "384044a"
---

# Phase 10 — Researcher Recommendations

## 1. Verdict

**REBASE — minor rework only.**

Phase 10's nine code-quality blocks are **framework-agnostic**. Eight of nine assess
**Fit: Full** with zero outdated async/API references. The single block needing
rewording is **Block 9 (Template and config drift)**, where two "API" references must
become "pipeline" references — the codebase has no API.

The required rework is four edits to the phase file:
1. **Lines 17–18 (scope boundary):** remove citations to deleted/vestigial phases
   (03, 09, 15); reword "lifecycle correctness (05)" → "import-time bootstrap and
   the settings gate (01)".
2. **Lines 106, 109 (Block 9):** "what the API receives" → "what the pipeline
   receives"; "the API request" → "the pipeline input".
3. **Line 131 (Report Output):** remove the stale "QLT- already in use" collision
   warning — disproven by grep (only 5 matches, all template placeholders from a
   different project; zero in shipped source).
4. **Line 131:** replace with corrected text stating `QLT-` is the Phase 10 prefix
   per template convention with no collision found.

No blocks are deleted. No blocks are rewritten. No severity taxonomy changes.

---

## 2. Proposed Final Block Set

Keep all 9 blocks. No additions, no deletions, no merges. All are framework-agnostic
lenses on convention, consistency, and enforcement inside the code itself.

| Block | Current title | Disposition | Rationale |
|---|---|---|---|
| 1 | Fixed values: one named home per value | **KEEP** | `StrEnum` vocabularies in `core/models.py` (`ActionType`, `MatchType`, `MappingColumns`, `DataFileExtension`); raw-literal duplicate sites in `core/db_service.py` and `core/utils.py`. Fully applicable; no async/API framing. |
| 2 | The constant surface as a maintained artifact | **KEEP** | Module-level constants (`VALID_COLUMN_DTYPES`, `DATA_TO_SQL_PARAMS`, `ALLOWED_PATTERN`, `AVAILABLE_ACTIONS`) all present and inspectable. No async/API framing. |
| 3 | Boundary validation coverage | **KEEP** | Boundaries are YAML file → `yaml_to_dict` → Pydantic model → `AppService` → SQLite. "What reaches persistence" is valid (SQLite is the persistence layer). The `extra` policy inconsistency (F4) is a direct instance. |
| 4 | Type width and the sanctioned-suppression surface | **KEEP** | `basedpyright` is the configured checker. Widened annotations (`str = None`, `bool = False` where `int | None` expected) are valid instances. |
| 5 | Responsibility inside a unit | **KEEP** | `AppService.run_report` (`app_service.py:110-290`) is a 180-line method performing orchestration, CSV ingestion, DB schema setup, rule-block matching, non-mapped extraction, index/data sync, and export — multiple responsibilities in one unit. |
| 6 | One rule, one home | **KEEP** | `AVAILABLE_ACTIONS` vs `ActionType`; `ALLOWED_PATTERN` vs `NameConstrained`; `_apply_delete` double execution. All valid duplication/divergence instances. |
| 7 | Convention compliance: the cheap high-volume rules | **KEEP** (strongest fit) | `sys.stdout.reconfigure` import-time side effect (`app.py:23-24`); bare `print()` on production path (`utils.py:34,41` via `app_service.py:113/257/272`; `db_service.py:308-309`); dead code (`merge_dicts`, `AVAILABLE_ACTIONS`, `get_table_sample`). |
| 8 | Declared contracts and their enforcement point | **KEEP** | `yaml_to_dict` docstring claims `{}`-on-error but no caller checks the cause (F5); `AVAILABLE_ACTIONS` declared but never enforced (F1); `_apply_delete` second execution (F9). |
| 9 | Template and config drift | **KEEP + REWORD** | Concept is valid. Reword "API" → "pipeline" at lines 106 and 109. No other changes. |

---

## 3. Candidate Findings — 13 Findings (F1–F13) Sorted by Severity with QLT- IDs

The auditor's 13 candidate findings are adopted verbatim. They are sorted by severity
band (HIGH → MEDIUM → LOW) and within each band by the auditor's original numbering
(F1–F13), preserving the block-based grouping. Each is assigned a `QLT-` ID in
sort order.

### HIGH severity (2 findings)

| QLT- ID | Orig. | Finding | Anchor | Ownership |
|---|---|---|---|---|
| QLT-001 | F5 | `yaml_to_dict` returns `{}` on `FileNotFoundError` or `YAMLError`, swallowing config errors — no caller checks the cause before constructing `DataSettings(**{})` → opaque `ConfigError` | `core/utils.py:131-149` | Phase 02 (root cause); Phase 10 (contract-enforcement gap) |
| QLT-002 | F13 | No drift detection gate exists for template↔Pydantic-model changes — 5 concrete drift cases documented by Phase 05 with zero automated detection | `core/models.py`, `app_config.yaml` | Phase 05 (template/model conformance); Phase 10 (missing enforcement point) |

### MEDIUM severity (5 findings)

| QLT- ID | Orig. | Finding | Anchor | Ownership |
|---|---|---|---|---|
| QLT-003 | F1 | `AVAILABLE_ACTIONS` dict (`db_service.py:39-43`) duplicates `ActionType` StrEnum (`models.py:106-109`) with zero call sites — two definitions of one rule, one dead | `core/db_service.py:39-43` vs `core/models.py:106-109` | Phase 10 (duplication); Phase 02 (dead code) |
| QLT-004 | F4 | `WorkingPaths` uses `extra="forbid"` while six sibling models use `extra="allow"` and two roots default to `extra="ignore"` — inconsistent validation boundary with no recorded reason | `core/models.py:61` (forbid) vs `models.py:46,78,85,94,135,143` (allow) vs `models.py:51,158` (ignore) | Phase 05 gap #7 (root cause); Phase 10 Block 3 |
| QLT-005 | F7 | `PolarsReadCSV.infer_schema_length` typed `bool = False` — Polars expects `int | None`; `False` → `0` → forces all columns to `String` | `core/models.py:102` | Phase 05 New 3 (root cause); Phase 10 Block 4 |
| QLT-006 | F10 | Bare `print()` on production path — `progress_bar` (`utils.py:34,41`) called from `AppService.run_report` (`app_service.py:219`); `get_table_sample` (`db_service.py:308-309`) also uses `print()` — bypasses `dictConfig`-configured logging stack | `core/utils.py:34,41`; `core/db_service.py:308-309` | Phase 10 (convention: stdout writes on production path) |
| QLT-007 | F11 | Mixed Russian/English output strings — `app_service.py:114` prints `Обработка стартовала` (Russian); `app_service.py:258` prints `Synchronizing the index table…` (English); `app_service.py:273` prints `Обработка завершена` (Russian). `app.py` module docstring (L2) and several Typer CLI help strings remain Russian while newer messages are English | `core/app_service.py:114,258,273`; `app.py:2,29,37,49,71` | Phase 10 (convention: language of log/error text) |

### LOW severity (6 findings)

| QLT- ID | Orig. | Finding | Anchor | Ownership |
|---|---|---|---|---|
| QLT-008 | F2 | `ALLOWED_PATTERN` regex (`utils.py:15`: `^[a-zA-Z_][a-zA-Z0-9_]*$`) duplicates `NameConstrained` regex (`models.py:29-36`) with identical pattern — two definitions of one validation rule | `core/utils.py:15` vs `core/models.py:29-36` | Phase 10 (one rule, one home) |
| QLT-009 | F3 | `DataFileExtension` defines `default = "csv"` and `csv = "csv"` — `csv` is an alias of `default` (StrEnum alias semantics) | `core/models.py:72-73` | Phase 05 gap #11 (root cause); Phase 10 Block 1 |
| QLT-010 | F6 | `DataFile.index_column` and `date_column` annotated `str` but defaulted to `None` — type checker disagrees; `None` flows into `DBWorker` target-table logic | `core/models.py:80-81` | Phase 10 (type width, Block 4) |
| QLT-011 | F8 | `__main__` block in `app_service.py:341-342` hardcodes Windows-specific data path `r"data\mvideo"` — dev-only execution block in an internal orchestrator module | `core/app_service.py:341-342` | Phase 10 (responsibility); Phase 01 (entry-layer discipline) |
| QLT-012 | F9 | `_apply_delete` (`db_service.py:630-643`) executes the identical DELETE statement twice via `perform_query` — second execution is a no-op but re-evaluates the subquery; correctness risk if future changes make DELETE non-idempotent | `core/db_service.py:630-643` | Phase 10 (one rule, one home) |
| QLT-013 | F12 | Dead code: `merge_dicts` (`utils.py:152-173`), `AVAILABLE_ACTIONS` (`db_service.py:39-43`), `get_table_sample` (`db_service.py:300-309`) — all present, reachable, referenced by nothing | `core/utils.py:152-173`; `core/db_service.py:39-43,300-309` | Phase 02 (dead code sweep); Phase 10 (records unreferenced definitions) |

---

## 4. Severity Taxonomy Assessment

**Verdict: Keep verbatim.**

Phase 10's severity taxonomy (lines 113–125 of the phase file) opens with the sound
principle: *"Grade by effect and blast radius, not by mechanism name; rate what is true
now, not the worst consequence if triggered. The bands are effect classes, not a closed
list of mechanisms."*

Every illustrative example is framework-agnostic and applies directly to the
synchronous, file→SQLite pipeline:

- **CRITICAL** — "a stored value that no rule ever constrained" (applies: `None` flowing into DB logic, gap #1); "a field default that overwrites stored content when the key is absent" (applies: SQLite `UPDATE` in `_sync_tables`, `db_service.py:388-426`); "two implementations of one rule whose copies already disagree on a value the system acts on" (applies: `AVAILABLE_ACTIONS` vs `ActionType`, though they agree today).
- **HIGH** — "a value with two homes" (applies: F1, F2); "schema drift no gate would notice" (applies: F13); "a declared invariant nothing enforces" (applies: F5).
- **MEDIUM** — "an unvalidated boundary on a non-critical path" (applies: `get_table_sample`); "a widened type on a value nothing branches on" (applies: F7); "a constant surface whose conventions are undocumented" (applies: `AVAILABLE_ACTIONS` ordering).
- **LOW** — "a declared rule no caller reaches" (applies: F1, F12); "a private name in a public export list" (applies: `app.py:157-164` `__all__` over-exports `get_app_service`, `app`, Typer command functions); "an unreferenced definition nobody has explained" (applies: F12).

**No examples reference async, API, event-loop, credentials, or retry.** No rewording
needed. The taxonomy is valid for the synchronous codebase as-is.

**Note:** The auditor's §5 references `app.py:157-164` for the `__all__` over-export
example. Verification confirms `__all__` spans lines 157–164 in the current file
(`app.py`, 165 lines total). The over-export of `get_app_service`, `app`, `init`,
`run`, and `list_settings` alongside the two contract functions (`process_data`,
`initialize_settings`) is confirmed.

---

## 5. Report Output Assessment

| Item | Current (Phase 10 §Report Output, lines 127–135) | Verdict | Notes |
|---|---|---|---|
| Findings path | `.ai/audit/10-code-quality/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`). Directory does not yet exist — no prior findings to reconcile. Confirmed absent via `glob`. |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template; front matter defines `severity_taxonomy` and `code_ref` fields. Verified present. |
| ID prefix | `QLT-` | **Keep** (correction required) | See §5.1 below. |
| problems-only | `true` (front matter L6 + §L134) | **Keep** | All 13 candidate findings are code-verified defects, not passing checks. |
| per-pass lines | ≤100, incremental append (L133) | **Keep** | Template constraint. |
| Empty state | `No problems found in this phase.` (L134) | **Keep** | Verbatim. |
| Shipped-test-as-blocker | L135: "a shipped test asserting current behaviour is a remediation blocker" | **Keep** | Applies to F5 → QLT-001 (`yaml_to_dict` `{}` fallback; if tests assert the mask, the fix is blocked until tests are updated). |

### 5.1 The `QLT-` prefix collision warning — DISPROVEN

Phase 10 line 131 states:
> Finding-ID prefix: `QLT-` — **already in use** in shipped source from a prior cycle, with at least one identifier already resolving to two different findings; check for a collision before minting an ID and report it rather than creating a second namespace.

**This claim is disproven by the working tree (code_ref `384044a`):**

- A repo-wide grep for `QLT-\d` returns **5 matches**, all in the **template**
  file `.ai/audit/templates/audit-findings.md` (lines 48, 49, 54, 72, 81) — these
  are **placeholder examples** in the generic template scaffold, using paths from a
  completely different project (`src/backend/apps/ads/models.py::Ad.transition_to`,
  `src/backend/apps/ads/services/submission.py::submit_ad`, `AUTZ-002`). They are
  part of *every* phase's starting document and are **not** finding IDs in any
  findings document.
- A repo-wide grep for `QLT-` (without `\d`) returns **6 matches**: the 5 template
  placeholders above, plus the Phase 10 phase file itself (line 131, the warning).
  (The auditor reported 7 matches including a `.ai/tmp/` scratch copy; verification
  confirms the scratch copy exists but is gitignored/non-shipped.)
- **No findings directory `.ai/audit/10-code-quality/` exists** — `glob` confirms
  only `templates/` and `problems/` are present under `.ai/audit/`.
- **No `.ai/audit/10-code-quality/findings.md` file exists.**
- **No shipped source file** (`.py`, `.yaml`, `.txt`) contains `QLT-`.

**Conclusion:** There is no "prior cycle" `QLT-` usage in shipped source. The template's
`QLT-001`/`QLT-002` are generic placeholders that ship inside the template scaffold
itself (used by all phases as the starting doc) — they use hardcoded example paths from a
Django ads app, which is why they do not correspond to real findings in *this* repo.
The Phase 10 phase file's warning is **stale phantom evidence** — it may have referred
to a prior audit cycle that has since been cleared, or may have conflated template
placeholders with actual findings.

**Recommendation:** Retain the `QLT-` prefix (it matches the template convention and is
unique among real findings — zero collisions found). **Remove the "already in use"
warning** on Phase 10 line 131. Replace with corrected text stating that `QLT-` is the
Phase 10 prefix per the template convention and that no collision was found in the
current tree.

---

## 6. Scope/Ownership & Citation Cleanup

### 6.1 Scope boundary (phase file line 17–18)

The current scope-boundary sentence spans lines 17–18 (the sentence wraps across the
line break at L17→L18). "Authorization (15)" appears on **line 18**, not line 17 — the
auditor's §3.2 note correctly identifies the task brief's "line 18" reference but
states the citation is on line 17; the actual file places it on line 18.

Current text (lines 17–18):
> Scope boundary — other phases own: settings values and environment policy (02); transaction, lock, and pooler semantics (03); lifecycle correctness (05); the event-loop bridge mechanism (09); test adequacy (11); pipeline and container posture (12); measurable performance consequences (13); authorization (15). This phase owns convention and consistency inside the code, and asks of every claimed property whether anything checks it — not what a lifecycle decision should be, what a request is allowed to do, or how the service is run and deployed.

| Cited phase | Current citation text | Status | Action | Rationale |
|---|---|---|---|---|
| (02) | "settings values and environment policy" | Re-based, exists | **KEEP** | Settings values & env policy are a real, synchronous concern owned by Phase 02. |
| (03) | "transaction, lock, and pooler semantics" | **Deleted** | **REMOVE** | Phase 03 auditor report §5 confirms deletion. Async-framed (Semaphore/Queue/worker-pool). SQLite transaction/lock semantics belong to Phase 04 (pipeline/DB layer), not Phase 10. |
| (05) | "lifecycle correctness" | Re-based, exists | **REWORD → (01)** | Phase 05's actual purpose is "Config Template Validation" (re-based from async). "Lifecycle correctness" is now Phase 01's domain — the lazy service factory (`get_app_service`) and the `_require_settings` gate (`app_service.py:307-338`). Reword to "import-time bootstrap and the settings gate (01)." |
| (09) | "the event-loop bridge mechanism" | **Deleted** | **REMOVE** | Phase 09 file already removed (researcher report §2 confirms). No event loop exists in the codebase. |
| (11) | "test adequacy" | Exists (sync) | **KEEP** | Test adequacy is a real concern for any codebase. |
| (12) | "pipeline and container posture" | Async-framed, to be re-based | **KEEP** | "Pipeline and container posture" is a real concern (observability, packaging). Phase 12 is async-framed but still exists; the concept survives re-basing. |
| (13) | "measurable performance consequences" | Async-framed, to be re-based | **KEEP** | "Measurable performance consequences" is a real concern (Polars I/O, SQLite bulk writes). Phase 13 is async-framed but still exists; the concept survives re-basing. |
| (15) | "authorization" | Async/API/credential-framed, deletion candidate | **REMOVE** | No credentials, no network, no auth in this codebase. Phase 15 ("Authorization & Credential Lifecycle") is a deletion candidate by the same precedent as Phase 03/09. Its scope citation to (15) must be removed from Phase 10. |

**Resulting scope boundary (recommended edit to lines 17–18):**
> Scope boundary — other phases own: settings values and environment policy (02);
> import-time bootstrap and the settings gate (01); test adequacy (11); pipeline
> and container posture (12); measurable performance consequences (13). This phase
> owns convention and consistency inside the code, and asks of every claimed property
> whether anything checks it — not what a lifecycle decision should be, what a
> request is allowed to do, or how the service is run and deployed.

**Note:** The phrase "what a request is allowed to do" (line 19) refers to authorization
(Phase 15's deleted domain). Since Phase 15 is being removed from the scope boundary,
this phrase becomes a dangling reference. **Minor recommendation:** reword to "what an
operation is permitted to do" or remove the clause entirely. The auditor §3.3 states
lines 18–21 need no changes, but this is a direct consequence of the (15) removal and
should be addressed for internal consistency.

### 6.2 Purpose paragraph (lines 13–15, 19–21) — no changes

The auditor's §3 assessment: lines 13–15 (Purpose) are framework-agnostic and keep
verbatim. Lines 19–21 (the rest of the purpose paragraph: "what a request is allowed to do",
"Two live implementations…", "17 establishes…", "16's measurement") are valid and
require no changes. The cross-references to Phase 17 (Arithmetic, line 20) and Phase 16
(line 21) are correct — Phase 17 exists and its purpose explicitly references Phase 10's
duplication detection; Phase 16 exists and owns rendered-surface/accessibility contracts.

### 6.3 Cross-phase citations of (10) — no action needed

The auditor's §7.2 confirms all downstream phases that cite (10) — Phases 02, 05, 11, 12,
13, 16, 17 — reference valid concerns owned by Phase 10. No citation cleanup is needed
in Phase 10's referencing phases. Phase 15 (which cited (10) at line 28) is a deletion
candidate; its citation disappears with the file. Phases 03 and 09 (which cited (10))
are already removed from the tree.

---

## 7. Edit Delta (line-by-line, for doc-specialist)

Target file: `.kilo/commands/audit/phases/10-audit-code-quality.md`

**Note on line numbers:** The auditor's report references "line 17" for the scope
boundary and "line 108" for the second "API" reference (§2 and §8). Verification
against the actual phase file shows: the scope-boundary sentence spans **lines 17–18**
(wraps at the line break), and "authorization (15)" appears on **line 18**. The second
"API" reference ("the API request") appears on **line 109**, not line 108 (the auditor's
§8 table correctly identifies line 109; §2 Block 9 incorrectly says line 108). The
edits below use the verified line numbers.

| # | Section | Lines | Current text | Action | Replacement text |
|---|---|---|---|---|---|
| 1 | Scope boundary | 17 | "...pooler semantics (03); lifecycle correctness (05); the event-loop bridge mechanism (09);..." | **Remove (03), reword (05)→(01), remove (09)** | "...import-time bootstrap and the settings gate (01);..." |
| 2 | Scope boundary | 18 | "posture (12); measurable performance consequences (13); authorization (15)." | **Remove (15)** | "posture (12); measurable performance consequences (13)." |
| 3 | Purpose continuation | 19 | "…not what a lifecycle decision should be, what a request is allowed to do, or how the service is run and deployed." | **Optional reword** | "…not what a lifecycle decision should be, what an operation is permitted to do, or how the service is run and deployed." |
| 4 | Purpose continuation | 20–21 | "the arithmetic showing whether they agree…; a contract that merely has no enforcement point is this phase's." | **No change** | (verbatim) |
| 5 | Block 9 — API reference | 106 | "…affects **what the API receives**, whether the change is recorded…" | **Reword API→pipeline** | "…affects **what the pipeline receives**, whether the change is recorded…" |
| 6 | Block 9 — API reference | 109 | "…whether a template key or model field that affects **the API request** can be added…" | **Reword API→pipeline** | "…whether a template key or model field that affects **the pipeline input** can be added…" |
| 7 | Report Output — ID prefix | 131–132 | "Finding-ID prefix: `QLT-` — **already in use** in shipped source from a prior cycle, with at least one identifier already resolving to two different findings; check for a collision before minting an ID and report it rather than creating a second namespace" | **Reword** | "Finding-ID prefix: `QLT-` — Phase 10 identifier prefix per the findings template convention. A repo-wide grep found no collision: `QLT-` appears only as generic placeholder IDs in `.ai/audit/templates/audit-findings.md` (a different project's example paths) and in this phase file's prior warning. No findings directory or findings file under `.ai/audit/10-code-quality/` exists; no shipped source file contains `QLT-`. Retain `QLT-` without a second namespace." |

**Application order:**
1. Edit 7 (front matter QLT- note) — removes stale phantom evidence first.
2. Edits 1–3 (scope boundary + purpose continuation) — removes dangling phase citations.
3. Edits 5–6 (Block 9 API→pipeline) — fixes Block 9 Fit from Partial to Full.
4. Edits 4 — no-op (documented as no change).

---

## 8. Findings-to-Scope Mapping (QLT-001 through QLT-013)

The 13 candidate findings are mapped below in **severity-sorted order** (the index order
IS the work order, per the findings template). Each `QLT-` ID is assigned in sort
position.

| QLT- ID | Orig. Finding | Severity | Block | Anchor |
|---|---|---|---|---|
| QLT-001 | F5 | HIGH | 3 / 8 | `core/utils.py:131-149` — `yaml_to_dict` `{}` passthrough |
| QLT-002 | F13 | HIGH | 9 | `core/models.py` + `app_config.yaml` — no drift detection gate |
| QLT-003 | F1 | MEDIUM | 1 / 6 | `core/db_service.py:39-43` vs `core/models.py:106-109` — `AVAILABLE_ACTIONS` vs `ActionType` |
| QLT-004 | F4 | MEDIUM | 3 | `core/models.py:61` vs `46,78,85,94,135,143` vs `51,158` — `extra` policy inconsistency |
| QLT-005 | F7 | MEDIUM | 4 | `core/models.py:102` — `infer_schema_length: bool = False` |
| QLT-006 | F10 | MEDIUM | 7 | `core/utils.py:34,41` (via `app_service.py:113/257/272`); `db_service.py:308-309` — bare `print()` |
| QLT-007 | F11 | MEDIUM | 7 | `core/app_service.py:114,258,273`; `app.py:2,29,37,49,71` — mixed Russian/English |
| QLT-008 | F2 | LOW | 1 / 6 | `core/utils.py:15` vs `core/models.py:29-36` — `ALLOWED_PATTERN` vs `NameConstrained` |
| QLT-009 | F3 | LOW | 1 | `core/models.py:72-73` — `DataFileExtension` duplicate values |
| QLT-010 | F6 | LOW | 4 | `core/models.py:80-81` — `str = None` annotations |
| QLT-011 | F8 | LOW | 5 | `core/app_service.py:341-342` — hardcoded Windows path in `__main__` |
| QLT-012 | F9 | LOW | 6 | `core/db_service.py:630-643` — `_apply_delete` double execution |
| QLT-013 | F12 | LOW | 7 | `core/utils.py:152-173`; `core/db_service.py:39-43,300-309` — dead code |

### Cross-finding dependencies

- **QLT-003 and QLT-013 overlap** on `AVAILABLE_ACTIONS` (`db_service.py:39-43`): QLT-003
  records the duplication (two definitions of one rule); QLT-013 records the dead-code
  aspect (zero call sites). Both are valid perspectives; the fix (delete the dict, use
  `ActionType` everywhere) resolves both.
- **QLT-001 and F5 → QLT-001** is the only finding with a shipped-test-as-blocker (Phase 10
  Report Output line 135): `tests/test_app.py:207-215` asserts the `{}` fallback behavior.
  The test must be updated alongside the fix.
- **QLT-002 is the highest-leverage finding**: it establishes the missing enforcement
  mechanism that would have caught QLT-004 (extra policy), QLT-005 (infer_schema_length
   type), and QLT-009 (DataFileExtension alias) as compile-time/template-time errors.

---

## 9. Verified Discrepancies (auditor report accuracy notes)

For transparency, three minor accuracy issues in the auditor report were identified
during source verification and do not affect the verdict:

1. **Missing `Database` model in F4 count.** The auditor states "six sibling models use
   `extra='allow'`" but lists only five line numbers (`78, 85, 94, 135, 143`). The sixth
   is `Database` at `models.py:46` (`ConfigDict(extra="allow")`). The finding's severity
   and ownership are unaffected; the `extra` policy inconsistency is confirmed.

2. **"authorization (15)" line number.** The auditor states `authorization (15)` appears
   on line 17 of the phase file. Verification shows the scope-boundary sentence starts
   on line 17 but wraps, and "authorization (15)" appears on **line 18**. This does not
   change the edit — only the line reference.

3. **F11 overstates `app.py` language.** The auditor states "all Typer CLI help strings
   are Russian" and cites `app.py:104`. Verification shows `list_settings` (line 104) has
   an English docstring: `"""Show settings files in the user settings directory"""`.
   The mixed-language finding (F11 → QLT-007) remains valid: the module docstring
   (line 2), `init` help (line 29), `init` docstring (line 49), `run` docstring (line 71),
   and several help strings are Russian, while `run`'s success/error messages
   (lines 82–84, 88) and `list_settings` are English. The commit `1be3956 style(app):
   use English strings in new CLI messages` confirms a partial migration to English.

---

## 10. Effort and Priority

**Effort: small.** Four line edits to the phase file (§7): one scope-boundary
reword, two "API"→"pipeline" substitutions, one front-matter note correction.
Thirteen candidate findings are ready to file in a new
`.ai/audit/10-code-quality/findings.md` following the template at
`.ai/audit/templates/audit-findings.md`.

**Priority: recommended.** Two of the edits address **actively misleading** content:
the citations to deleted phases (03, 09) and the vestigial authorization (15) in the
scope boundary, plus the stale "QLT- already in use" collision warning (line 131)
that falsely implies a namespace conflict. The Block 9 "API"→"pipeline" reword
restores Block 9 from Fit: Partial to Fit: Full.

No source files are modified by this report. The 13 findings are documented for
filing when Phase 10 is formally executed; several have root causes owned by other
phases (QLT-001 → Phase 02, QLT-005 → Phase 05, QLT-011 → Phase 01) — Phase 10's
role is to record the duplication/missing-enforcement signal and cross-reference
the owning phase, not double-fix.
