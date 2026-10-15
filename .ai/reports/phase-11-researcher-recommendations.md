---
phase: "11"
phase_name: "Test Coverage"
phase_file: ".kilo/commands/audit/phases/11-audit-test-coverage.md"
source: ".ai/reports/phase-11-auditor-report.md"
report_status: "draft"
recommendation: REBASE
code_ref: "384044a"
---

# Phase 11 — Researcher Recommendations

## 1. Verdict

**REBASE — minor rework only.**

All 8 blocks in the Phase 11 phase file are framework-agnostic principles applicable to
the synchronous, pytest-based test suite. Zero blocks need deletion. Two blocks (5, 7)
need targeted rewording to remove presumptive infrastructure references; the remaining
six need only trivial or no changes.

Rework scope: **7 edits to the phase file only** — no source files modified, no tests
modified.

---

## 2. Proposed Final Block Set (all 8 kept)

| Block | Current title | Disposition | Phase-file edit |
|---|---|---|---|
| 1 | Exercised, not merely reachable | KEEP (verbatim) | none |
| 2 | What the doubles remove | KEEP + REWORD | L41: "scheduling primitive" → "ordering mechanism" |
| 3 | The fixture data contract | KEEP (verbatim) | none |
| 4 | Both sides of every boundary | KEEP (verbatim) | none |
| 5 | The harness as a system | KEEP + REWORD | L70–73: 5 presumptive phrases → conditional checks |
| 6 | Reproducibility and debuggability | KEEP + REWORD | L83: "a shuffling plugin" → general phrasing |
| 7 | What actually blocks a merge | KEEP + REWORD | L90: gate list → project-aware conditional |
| 8 | Declared test-layer contracts | KEEP (verbatim) | none |

---

## 3. Confirmed Findings (TST- IDs)

### TST-001 — `slow` marker registered but never used — LOW

**Anchor:** `pyproject.toml:167::markers[0]`

**Evidence — `pyproject.toml:166-168`:**
```toml
markers = [
    "slow: marks tests as slow (use -m slow to run)",
]
```

**Evidence — `rg -n '@pytest.mark' tests/` (repo root):**
```
(no matches — 0 files, 0 occurrences)
```

**Problem:** The `slow` marker is registered under `[tool.pytest.ini_options]`
(`pyproject.toml:167`) so `--strict-markers` will accept `@pytest.mark.slow`, but no
test in `tests/` (7 files: `conftest.py`, `test_app.py`, `test_csv_service.py`,
`test_db_service.py`, `test_dict_service.py`, `test_metadata.py`, `test_notebook.py`)
ever applies it. The `--import-mode=importlib` mode is also commented out
(`pyproject.toml:160`: `#    "--asyncio-mode=auto"`).

**Severity:** LOW — "a marker registered and inert" per the Phase 11 taxonomy
(L121 of phase file). Self-description/hygiene; no runtime consequence today. A
future audit under Block 8 should check whether the marker was *intended* for use
(slow-suite gating) and either wire it or remove it.

**Verify:** no test currently uses `@pytest.mark` at all — the marker's inert state is
confirmed by grep, not by absence of `@pytest.mark.slow` specifically.

---

## 4. Phase-File Rework Findings (not filed — doc-only edits)

These four items are defects in the *phase file itself* (`.kilo/commands/audit/phases/11-audit-test-coverage.md`),
not in shipped source. They are corrected by the edits in §7 and are not assigned TST-
IDs.

| Issue | Phase-file line | Evidence |
|---|---|---|
| Stale scope citation (03) | L18 | Phase 03 phase file (`03-audit-*.md`) is **absent** from `.kilo/commands/audit/phases/` — deleted. Only `phase-03-auditor-report.md` and `phase-03-researcher-recommendations.md` remain in `.ai/reports/`. Phase 03 owned "Async Concurrency & Worker Safety" (Semaphore/Queue/worker-pool); the project has zero async primitives (grep for `asyncio\|Semaphore\|Queue` in `src/mko_data_cleaner/` returns 0 matches). |
| Stale scope citation (05) | L18 | Phase 05's purpose is "Config Template Validation" (`05-audit-config-templates.md:11`): it does **not** own "lifecycle correctness." Verified: Phase 01 scope boundary (`01-audit-entry-architecture.md:24-27`) owns "import-time behaviour, configuration bootstrap" and "the lazy, opt-in settings gate." Per Phase 10 researcher rec §6.1 and Phase 10 auditor §3: lifecycle correctness → (01). |
| "container" in scope citation (12) | L18 | No containers anywhere in this project. `architecture.md:37`: "External systems: none." No `docker-compose*`, no `Dockerfile`, no `Makefile` in repo. Phase 12's own purpose (L17 of `12-audit-production-ops.md`) says "There is no edge tier, no transport security boundary, no health-probe contract, and no backup." Reword "container posture" → "operational posture." |
| TST- collision warning | L128–129 | See §5. Disproven: `rg -n 'TST-' --no-ignore` returns 2 shipped matches — both the Phase 11 phase file itself (L128) and a non-tracked `.ai/tmp/scratch/` copy. Zero in any `.py`, `.yaml`, `.toml`, `.md` report, or findings file. |

---

## 5. The `TST-` Prefix Collision Warning — DISPROVEN

Phase 11 line 128 states:

> Finding-ID prefix: `TST-` — **already in use** in shipped source and operations
> documentation from a prior cycle, with at least one identifier already resolving
> to two different findings; check for a collision before minting an ID and report
> it rather than creating a second namespace.

**Disproven by the working tree (code_ref `384044a`).**

Repo-wide `rg -n 'TST-'`:

| Match location | Tracked? | Contains a real finding ID? |
|---|---|---|
| `.kilo/commands/audit/phases/11-audit-test-coverage.md:128` | Yes | No — this is the warning text itself |
| `.ai/tmp/scratch/.kilo/commands/audit/phases/11-audit-test-coverage.md:128` | No (gitignored scratch) | No — non-tracked copy of the warning text |
| `.ai/reports/phase-11-auditor-report.md` (8 matches) | Yes | No — auditor report analyzing the warning |

- **0 matches** in any shipped source file (`.py`, `.yaml`, `.toml`, `.lock`)
- **0 matches** in any findings document or report outside the auditor's own §5 analysis
- **0 matches** in `.ai/audit/templates/audit-findings.md` (the template uses `QLT-`
  placeholders, L48–81 — different project's example paths)
- **0 matches** in `.ai/audit/` outside the template's `QLT-` scaffold
- **No** `.ai/audit/11-test-coverage/` directory exists (`Test-Path` → False; `glob .ai/audit/**` shows only `templates/` and `problems/`)

The `audit-findings.md` template (`id_prefix: "{PREFIX}"`, L4) reads the prefix from
the phase file; there is no prefix list in the template. The `QLT-` placeholders in
the template (L48–49, L54, L72, L81) are generic scaffold examples using paths from
a Django ads app — not real findings in this repo, and not `TST-`.

**Conclusion:** Same root cause as the Phase 10 `QLT-` phantom warning (Phase 10
researcher rec §5.1): the warning likely referred to a prior audit cycle that has
since been cleared, or conflated template placeholders with actual findings. No
`TST-` IDs have ever been minted in this tree.

**Recommendation:** Retain `TST-` (unique, matches template convention). Remove the
"already in use" warning on L128. Replace with corrected text:

> Finding-ID prefix: `TST-` — Phase 11 identifier prefix per the findings template
> convention. A repo-wide grep found no collision: `TST-` appears only in this phase
> file's prior warning and a non-tracked scratch copy. No findings directory or
> findings file under `.ai/audit/11-test-coverage/` exists; no shipped source file
> contains `TST-`. Retain `TST-` without a second namespace.

---

## 6. Severity Taxonomy Assessment

**Verdict: Keep verbatim, one-word reword.**

Phase 11's severity taxonomy (phase file L110–122) opens with the sound principle:
*"Grade by effect and blast radius, not by mechanism name; rate what is true now,
not the worst consequence if triggered. The bands are effect classes, not a closed
list of mechanisms."*

- **CRITICAL (L115–116):** "one a user or **a seller** receives" — "seller" is
  advertising-domain jargon from the Mediascope context. Verified: the project
  post-processes TV advertising exports (`docs/00-overview/architecture.md:24-40`);
  the system-context diagram (L28) shows `Analyst[Media / data analyst]` as the
  sole external actor (L37–38: "the only external actor is the analyst"). The
  output is consumed by a data engineer or analyst, not a "seller." **Edit L116:**
  "seller" → "operator". All other CRITICAL examples are framework-agnostic.
- **HIGH (L117–118):** "a gate declared and never loaded" — directly maps to the
  absent coverage/translation/security/CI gates (see §7.4). No rewording needed.
- **MEDIUM (L119–120):** "a resource left unbounded" applies to SQLite temp files
  via `tmp_path`; "state carried forward between runs" applies to temp DB files
  if cleanup fails. No rewording needed.
- **LOW (L121–122):** "a marker registered and inert" — **directly applicable**:
  the `slow` marker is registered at `pyproject.toml:167` but never used (TST-001,
  §3). No rewording needed.

**Note on format inconsistency:** Phase 11 uses prose-list format for the taxonomy
(L115–122), while Phase 02 uses a table. Phase 99 §8 should standardize the
template format. Not a Phase 11 action.

---

## 7. Report Output Assessment

| Item | Current (phase file L126–133) | Verdict | Notes |
|---|---|---|---|
| Findings path | `.ai/audit/11-test-coverage/findings.md` | **KEEP** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 02, 04, 05, 10). Directory does not exist yet — no prior findings to reconcile. |
| Template | `.ai/audit/templates/audit-findings.md` | **KEEP** | Verified present (108 lines). Front matter defines `id_prefix`, `severity_taxonomy`, `code_ref` fields. |
| ID prefix | `TST-` | **KEEP** (correction required) | See §5. Remove phantom collision warning on L128. |
| Incremental append | ≤100 lines per pass (L130) | **KEEP** | Verified in template L105. |
| problems-only | `true` (L6 + L131) | **KEEP** | All findings are code-verified defects. |
| Empty state | `No problems found in this phase.` (L132) | **KEEP** | Verbatim. |
| Shipped-test-as-blocker | L133 | **KEEP** | Directly applicable if a finding depends on current test behaviour. |

---

## 8. Block-by-Block Edit Detail

### Block 2 (L41)

**Current:** "...a stubbed deferral or scheduling primitive, an in-memory stand-in..."
**Issue:** "Scheduling primitive" leans toward async schedulers (asyncio tasks,
Celery). The project has no scheduler — work is a straight synchronous call chain
(`app.py → AppService.run_report → CSVWorker → DBWorker`).
**Verified test doubles:** `monkeypatch` (`test_app.py:205, 304, 375`);
`SimpleNamespace` stub (`test_app.py:303`); `CliRunner` transport double
(`test_app.py:169, 181, 191, 207, 358`); in-memory SQLite via `tmp_path`
(`conftest.py:34–44, 102–107`).
**Edit:** "scheduling primitive" → "ordering mechanism" (one-word swap).
**Discrepancy note:** The auditor references this as "L43" (summary table L93, §2
Block 2 L116). The actual phase-file line is **L41**. L43 reads "is not this block's
finding; replacing the mechanism under test removes the subject" — it contains the
word "ordering" but not "scheduling primitive." This is a minor line-number
inaccuracy in the auditor report; the edit target is L41.

### Block 5 (L70–73) — 5 presumptive phrases → conditional checks

| Phase-file line | Current phrase | Issue | Edit |
|---|---|---|---|
| L70 | "how **parallel execution** partitions and isolates" | No xdist, no `-n auto`, no `--maxprocesses` | → "how execution is partitioned and isolated" |
| L71 | "whether the **two execution paths this project supports** reach the same runtime configuration" | Only one execution path exists: `pytest` with default `addopts`. The `slow` marker is registered but never used — no fast/slow split. | → "whether multiple execution paths exist and, if so, whether they reach the same runtime configuration" |
| L72 | "a step excluded from the **fast path**" | No fast path exists. | → "a step excluded from any fast-path run, where nothing detects the failures it would otherwise surface" |
| L73 | "**two helper universes** separated by an execution model neither can cross" | Only `tests/conftest.py` exists as the fixture source. No second helper universe. | → "multiple helper configurations not interoperable with each other" |
| L73 | "**an import-path shim** retained so that existing patch targets still resolve after their module moved" | No modules have moved. `--import-mode=importlib` (L156 of `pyproject.toml`) is set, but no `sys.path` hacks exist. | → Make conditional: "an import-path shim retained so that existing patch targets still resolve, if any exist." |

**L75 (config template or model):** Auditor says KEEP. Verified: Phase 11 L75 says
"what a change to a config template or model does to the run." Pydantic v2 models
are used (`core/models.py`). The concept is valid. No edit.

### Block 6 (L83)

**Current:** "A **shuffling plugin** makes no individual test independent"
**Issue:** No `pytest-randomly` or `pytest-random-order` in dependencies. Not even
listed in `pyproject.toml`'s dev group. `rg -n 'pytest-randomly\|pytest-random-order'`
returns 0 matches in `pyproject.toml` and `uv.lock`.
**Edit:** "A shuffling plugin makes no individual test independent" →
"Reordering tests does not make an order-dependent test independent."
Preserves the principle (order can create false independence) without presuming a tool.

### Block 7 (L90)

**Current:** "For each declared gate — static analysis, types, tests, coverage,
translation completeness, security and dependency and secret scanning, deploy check,
scheduled suites — establish whether it is loaded…"

**Verified gate inventory (`pyproject.toml` + repo):**

| Gate type | Declared? | Loaded? | Notes |
|---|---|---|---|
| Static analysis (ruff) | Yes — `[tool.ruff.lint]` L111 | Yes | `ruff check` runs locally |
| Types (mypy) | Yes — `[tool.mypy]` L140 | Yes | `mypy` configured |
| Tests (pytest) | Yes — `[tool.pytest.ini_options]` L152 | Yes | Suite passes |
| Coverage | No | No | No `coverage` plugin in `pyproject.toml` |
| Translation completeness | No | No | No i18n infra yet (project rules §16: future gate) |
| Security/dependency/secret scanning | No | No | No `.github/`, no `bandit`, `pip-audit`, `trivy` |
| Deploy check | No | No | No deployment, no CI/CD pipeline |
| Scheduled suites | No | No | No CI/CD pipeline |

**Issue:** Listing "security and dependency and secret scanning, deploy check,
scheduled suites" alongside "static analysis, types, tests" implies gate parity
that cannot exist for a local-only, no-network, no-credentials CLI tool.
**Edit:** Reword to make absent categories conditional. Replace the gate list on
L90 with:

> "For each declared gate — static analysis, types, tests, coverage, translation
> completeness, or any other gate declared in the project's configuration — establish
> whether it is loaded, whether it is green, and what its scope excludes. For this
> project, several gate categories are inherently absent by design: no security,
> dependency, or secret scanning (local-only, no network, no credentials); no
> deploy check (no deployment); no scheduled suites (no CI/CD pipeline).
> Translation completeness is a future gate per project rules §16.

### Severity taxonomy (L116)

**Current:** "one a user or a seller receives."
**Edit:** "seller" → "operator" (one-word reword). All other taxonomy text stays
verbatim (§6).

### Report Output (L128–129)

**Current:** `TST-` — **already in use** in shipped source… (phantom collision warning)
**Edit:** See §5 — replace with corrected text stating `TST-` is the Phase 11 prefix,
no collision found in current tree.

---

## 9. Edit Delta — 7 edits to `.kilo/commands/audit/phases/11-audit-test-coverage.md`

| # | Section | Lines | Current text (exact excerpt) | Action | Replacement text |
|---|---|---|---|---|---|
| 1 | Scope boundary | L18 | "…pipeline and **container** posture (12); **transaction, lock, and pooler semantics (03); lifecycle correctness (05);**…" | Remove (03), reword (05)→(01), remove "container" from (12) | "…pipeline and **operational** posture (12); **import-time bootstrap and the settings gate (01);**…" |
| 2 | Scope boundary (optional) | L18 | "(01);" (after edit 1) | ADD (04) citation | "(01); DB lifecycle and pipeline teardown in fixtures (04);" |
| 3 | Block 2 | L41 | "a stubbed deferral or **scheduling primitive**" | Reword | "a stubbed deferral or **ordering mechanism**" |
| 4 | Block 5 | L70 | "**how parallel execution** partitions and isolates" | Reword → conditional | "**how execution** is partitioned and isolated" |
| 5 | Block 5 | L71 | "**whether the two execution paths this project supports**" | Reword → conditional | "**whether multiple execution paths exist and, if so, whether they**" |
| 6 | Block 5 | L72 | "**a step excluded from the fast path**" | Reword → conditional | "**a step excluded from any fast-path run, where nothing detects the failures it would otherwise surface**" |
| 7 | Block 5 | L73 | "**two helper universes** separated by an execution model neither can cross; an **import-path shim** retained so that existing patch targets still resolve after their module moved" | Reword → conditional | "**multiple helper configurations** not interoperable with each other; an **import-path shim** retained so that existing patch targets still resolve, if any exist" |
| 8 | Block 6 | L83 | "**A shuffling plugin** makes no individual test independent" | Reword → general | "**Reordering tests** does not make an order-dependent test independent" |
| 9 | Block 7 | L90 | "…**security and dependency and secret scanning, deploy check, scheduled suites** — establish whether it is loaded…" | Reword → project-aware conditional | "…**or any other gate declared in the project's configuration** — establish whether it is loaded… For this project, several gate categories are inherently absent by design: no security, dependency, or secret scanning (local-only, no network, no credentials); no deploy check; no scheduled suites." |
| 10 | Severity taxonomy | L116 | "one a user or a **seller** receives" | Reword | "one a user or an **operator** receives" |
| 11 | Report Output | L128–129 | "Finding-ID prefix: `TST-` — **already in use** in shipped source and operations documentation from a prior cycle, with at least one identifier already resolving to two different findings; check for a collision before minting an ID and report it rather than creating a second namespace" | Reword (remove warning) | "Finding-ID prefix: `TST-` — Phase 11 identifier prefix per the findings template convention. A repo-wide grep found no collision: `TST-` appears only in this phase file's prior warning and a non-tracked scratch copy. No findings directory exists under `.ai/audit/11-test-coverage/`; no shipped source file contains `TST-`. Retain `TST-` without a second namespace." |

**Edit count: 11 lines touched across 7 logical changes** (Block 5 spans L70–73 =
4 lines but is one logical reword; TST- spans L128–129 = 2 lines but is one logical
change). This falls within the auditor's "≤8 line edits" estimate (counting logical
blocks, not raw lines).

**Application order:**
1. Edits 1–2 (scope boundary) — removes stale (03), rewords (05)→(01), fixes (12).
2. Edit 3 (Block 2) — trivial one-word swap.
3. Edits 4–7 (Block 5) — sequential, top-to-bottom within the block.
4. Edit 8 (Block 6) — one-line swap.
5. Edit 9 (Block 7) — gate-list reword.
6. Edit 10 (severity taxonomy) — one-word swap.
7. Edit 11 (Report Output) — removes phantom evidence.

---

## 10. Cross-Phase Citations of (11) — Verified

The auditor's §8 table lists Phases 04, 12, 13, 16, 17 as citing (11). **Verification
against phase files** confirms:

| Citing phase | Phase file line | Citation text | Verdict |
|---|---|---|---|
| 12 (`12-audit-production-ops.md`) | L25–26 | "test-suite adequacy (11)" | **KEEP** — in scope boundary |
| 13 (`13-audit-performance.md`) | L27 | "test-suite adequacy (11)" | **KEEP** — in scope boundary |
| 16 (`16-audit-rendered-surface.md`) | L27 | "test-suite adequacy (11)" | **KEEP** — in scope boundary |
| 17 (`17-audit-arithmetic.md`) | L28 | "test-suite adequacy (11)" | **KEEP** — in scope boundary |
| 04 (`04-audit-data-pipeline.md`) | — | Not cited in scope boundary | **N/A** — auditor §8 correctly notes Phase 04 does not cite (11); the (04)→(11) relationship is one-directional (Phase 11 optionally cites (04), see edit 2 above) |

**Auditor §8 gap — verification found additional citation:** Phase 10
(`10-audit-code-quality.md` L17) cites "(11)" as "test adequacy (11)" in its scope
boundary. The auditor's §8 table does not list Phase 10. Phase 10 is already
re-based (sync) and its auditor report §6 correctly shows the (11) citation marked
KEEP (`phase-10-auditor-report.md:403`). Phase 09
(`phase-09-researcher-recommendations.md` L53) and Phase 01
(`01-audit-entry-architecture.md:27`) also reference (11) — Phase 09 as "test adequacy
(11)" in a scope reword, Phase 01 as "test-suite entry points (11)" in an
out-of-scope exclusion. All are valid.

**Conclusion:** All downstream (11) citations are valid references to test-suite
adequacy. No citation cleanup is needed in referencing phases. The auditor's §8
conclusion is correct; its enumeration was incomplete (missed Phase 10's "test
adequacy (11)" phrasing).

---

## 11. Verified Discrepancies in Auditor Report

Three minor accuracy issues — none affect the REBASE verdict or the edit set:

1. **Block 2 line number (§2 Block 2, §9 item 5).** Auditor cites "L43" for
   "scheduling primitive." The actual phase-file line is **L41** (verified by direct
   file read). L43 contains "ordering" but not "scheduling primitive."

2. **`yaml_to_dict` test line range (§2 Block 8, L229).** Auditor cites
   "test_app.py:207-221" for the `yaml_to_dict` returns-`{}` tests. The actual tests
   are at **L242–251** (`test_yaml_to_dict_returns_empty_dict_on_missing_file` L242,
   `test_yaml_to_dict_returns_empty_dict_on_invalid_yaml` L247). Lines 207–221 are
   `test_init_cli_creates_settings_in_tmp_dir` / `test_get_app_service_constructs_after_init`.
   The substance (tests exist that assert the `{}` fallback) is correct; only the
   line range is wrong.

3. **§8 cross-phase enumeration incompleteness.** Auditor §8 lists only 04, 12, 13,
   16, 17 as citing (11). Verification found Phase 10 also cites (11) at phase file
   L17 ("test adequacy (11)"). Conclusion unchanged — all citations are valid and
   should be kept.

---

## 12. Effort and Priority

**Effort: small.** 7 logical edits to the phase file (11 raw lines touched). One
actual code finding (TST-001, `slow` marker) ready to file in
`.ai/audit/11-test-coverage/findings.md`.

**Priority: recommended.** Three edits address actively misleading content:
- L18: stale citation to deleted Phase 03 in the scope boundary
- L128–129: phantom `TST-` collision warning that could deter an auditor from minting
  `TST-` IDs
- L90: gate list that implies security/dependency scanning gates exist where they
  cannot

**No source code, test files, or findings documents are modified by this report.**
The single code finding (TST-001) is documented for filing when Phase 11 is formally
executed.
