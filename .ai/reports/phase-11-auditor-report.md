---
phase: "11"
phase_name: "Test Coverage"
phase_file: ".kilo/commands/audit/phases/11-audit-test-coverage.md"
report_status: "draft"
scope: >-
  re-basing assessment of Phase 11 against a synchronous, single-shot,
  local-only CLI + library with no event loop, no API, no network, no
  credentials, and no containers. The test suite is a plain pytest 9.0.2
  configuration with no xdist, no seed marker, no CI/CD, and no security
  scanning.
code_ref: "384044a"
recommendation: REBASE
---

# Audit Report — Phase 11 Test Coverage (Rework Assessment)

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **synchronous, single-shot, local-only** CLI tool and
Python library that post-processes Mediascope TV advertising exports received as
**files on disk**. There is no outbound API, no credentials, no network, no event
loop, no async, and no containers anywhere in the architecture. The runtime is a
straight synchronous call chain:

```
app.py → AppService.run_report → CSVWorker → DBWorker / MappingDict
```

The test suite is grounded in `pyproject.toml` and `tests/`:

```toml
[tool.pytest.ini_options]
addopts = ["--import-mode=importlib", "-ra", "-v", "--strict-markers"]
testpaths = ["tests"]
pythonpath = ["src"]
markers = ["slow: marks tests as slow (use -m slow to run)"]
```

Code-verified facts (`src/mko_data_cleaner/`, `tests/`, `pyproject.toml`):

- **No parallel execution.** No `pytest-xdist`, no `-n auto`, no `--maxprocesses`
  in `pyproject.toml` or dev dependencies. The only `--strict-markers` marker
  registered is `slow` (defined at `pyproject.toml:167`); it is **never used** —
  zero `@pytest.mark.slow` or `@pytest.mark` occurrences across `tests/*.py`
  (grep: 0 matches).
- **No `seed` marker, no fast/short gate split.** `PYTEST_SKIP_MARKERS=seed` and
  `-n auto` appear only in `.kilo/rules/commands.md` (Kilo agent tooling for a
  *different* project — `mko-bazuna-test`) and in `.ai/context/test-command-patterns.md`.
  They do **not** appear in the shipped `pyproject.toml` or `tests/`.
- **No `src/backend/` directory.** Tests live in `tests/` at the repository root
  (`tests/conftest.py`, `tests/test_app.py`, `tests/test_csv_service.py`,
  `tests/test_db_service.py`, `tests/test_dict_service.py`, `tests/test_metadata.py`,
  `tests/test_notebook.py`). The `src/backend/conftest.py` reference in the task
  brief describes Kilo infrastructure, not this project.
- **No Docker, no PostgreSQL, no `.env*`.** No `docker-compose*`, no `Makefile`,
  no `.env.test` in the repo. The only database is a disposable SQLite file per
  invocation (`DBWorker` context manager: checkpoint WAL + delete on exit).
- **No CI/CD, no coverage gate, no security/dependency/secret scanning.** No
  `.github/` directory exists. No `coverage` plugin in `pyproject.toml`. No
  `bandit`, `pip-audit`, `trivy`, or secret-scan tool configured. The project is
  explicitly local-only: `architecture.md:37-40` states "External systems: none."
- **No shuffling plugin.** No `pytest-randomly` or `pytest-random-order` in
  dependencies.
- **One autouse fixture.** `test_app.py:55` defines `_isolate_logging` (autouse,
  restores logging state), not `conftest.py`.

**Conclusion for Phase 11:** The eight test-coverage blocks are **framework-agnostic
principles** — each is a lens on assurance fidelity, not a checklist tied to a
specific framework. Unlike Phase 03 (async concurrency, deleted) or Phase 09 (external
API, deleted), Phase 11 has **zero blocks to delete** and **no block whose premise
is entirely fictional**. However, several blocks make **assumptions about a richer
test infrastructure** (two execution paths, parallel execution, fast-path exclusion,
import-path shims, secret/dependency scanning gates) that do not exist in this
project. These need targeted rewording so the blocks ask the auditor to *check* rather
than *presume*.

The required rework is **minor-to-moderate citation cleanup and rewording only**.

---

## 2. Per-Block Assessment (1–8)

Each block is assessed for **Fit** (does the concept apply to the real test
suite), **Outdated** (async/API/infra-specific references that need rewording),
and **Replacement** (what changes are needed).

### Summary table

| Block | Current title | Fit | Outdated references | Replacement needed |
|---|---|---|---|---|
| 1 | Exercised, not merely reachable | **Full** | None | No changes |
| 2 | What the doubles remove | **Full** | "scheduling primitive" (L43) — leans async | Reword to "ordering mechanism" |
| 3 | The fixture data contract | **Full** | None | No changes |
| 4 | Both sides of every boundary | **Full** | None | No changes |
| 5 | The harness as a system | **Partial** | "parallel execution" (L70); "two execution paths this project supports" (L71); "fast path" (L72); "two helper universes" (L73); "import-path shim" (L73) | Reword assumptions → conditional checks |
| 6 | Reproducibility and debuggability | **Full** | "a shuffling plugin" (L83) — no shuffling plugin installed | Minor rewording for generality |
| 7 | What actually blocks a merge | **Full** | "secret scanning", "dependency scanning", "deploy check", "scheduled suites" (all absent) (L90) | Reword gate list to be project-aware |
| 8 | Declared test-layer contracts | **Full** | None | No changes |

### Block 1 — Exercised, not merely reachable
- **Fit: Full.** The concept (derive the decision set from code; for each decision,
  what value would assertions reject; coverage that survives inversion) is universal.
  The project's decisions live in `AppService.run_report` (`app_service.py:110-290`),
  `CSVWorker`, `DBWorker`, and rule-block processing — all synchronous and directly
  inspectable.
- **Outdated:** None. No async/API/Mediascope-domain framing.
- **Replacement:** No changes.

### Block 2 — What the doubles remove
- **Fit: Full.** The concept (inventory every test double; establish what property
  of the real thing is no longer observed) applies directly. The project uses
  `monkeypatch` (e.g., `test_app.py:204, 304, 373`), `SimpleNamespace` stubs
  (`test_app.py:303`), `CliRunner` (transport double), and in-memory SQLite via
  `tmp_path` (`conftest.py:34-44`).
- **Outdated:** "a stubbed deferral or **scheduling primitive**" (L43). "Scheduling
  primitive" leans toward async schedulers (asyncio tasks, Celery, etc.). In a
  synchronous single-shot CLI, there are no scheduling primitives to stub — work
  is a straight call chain.
- **Replacement:** Reword "scheduling primitive" → "ordering mechanism" (covers
  synchronous ordering concerns, e.g., a double that affects when work happens,
  without presuming async schedulers). One-word swap.

### Block 3 — The fixture data contract
- **Fit: Full.** The concept (what states and values test helpers manufacture by
  default; whether the product can produce each; whether a bare literal restates
  a named value) applies directly. The project uses Pydantic v2 models
  (`core/models.py`) and fixtures in `tests/conftest.py` that hardcode config
  (`realistic_config`) and data (`sample_dictionary`, `sample_mapping_df`).
- **Outdated:** None. The Pydantic reference is on-point. The "secrets" clause
  (L55: "whether helper data carries real identifiers or secrets") is a valid
  hygiene check even for a local-only project (a future i18n/key-management
  concern per project rules §16).
- **Replacement:** No changes.

### Block 4 — Both sides of every boundary
- **Fit: Full.** The concept (process, transaction, storage, cache, clock, file
  system boundaries — assert from each side) applies directly. The project's
  boundaries: file system → `CSVWorker` → SQLite (via `DBWorker` context manager,
  `db_service.py`); `datetime`/`time` clock in `AppService` timing.
- **Outdated:** None. "Cache" is listed as one possible boundary among several;
  the block says "for each boundary the architecture creates" — if no cache
  exists, the auditor skips it. Correct framing.
- **Replacement:** No changes.

### Block 5 — The harness as a system
- **Fit: Partial.** The core principles (fixture setup, mock configuration, marker
  wiring, override-substitution semantics, harness limitations) are valid. But
  several phrases **assume** infrastructure that does not exist in this project.
- **Outdated:**
  - L70: "how parallel execution partitions and isolates" — no parallel execution
    (no xdist, no `-n auto`).
  - L71: "whether the two execution paths this project supports reach the same
    runtime configuration" — **assumes** two execution paths. The project has one:
    `pytest` with default `addopts`. The `slow` marker is registered but never
    used; there is no fast-path/slow-path split. (The "two execution paths"
    concept exists only in Kilo infrastructure for a different project.)
  - L72: "a step excluded from the fast path" — no fast path exists.
  - L73: "two helper universes separated by an execution model neither can cross"
    — no multiple helper universes exist. `tests/conftest.py` is the sole fixture
    source; `test_app.py:55` has one autouse fixture (`_isolate_logging`).
  - L73: "an import-path shim retained so that existing patch targets still resolve
    after their module moved" — no import-path shuffims exist. `--import-mode=importlib`
    is set (L156 of `pyproject.toml`), but no modules have moved.
  - L75: "config template or model" — overlaps with Phase 05 scope; the phrasing
    is Django-leaning ("model"), but Pydantic models are valid here too.
- **Replacement:** Reword to make these conditional rather than presumptive:
  - L70: "how parallel execution partitions and isolates" → "how execution is
    partitioned and isolated" (works for serial and parallel).
  - L71: "the two execution paths this project supports" → "whether multiple
    execution paths exist and, if so, whether they reach the same runtime
    configuration."
  - L72: "a step excluded from the fast path" → "a step excluded from any
    fast-path run, where nothing detects the failures it would otherwise surface."
  - L73: "two helper universes separated by an execution model" → "multiple
    helper configurations not interoperable with each other."
  - L73: "an import-path shim retained" → "an import-path shim retained for
    backward patch-target resolution." (Conditional — if any exist.)
  - L75: Keep as-is (overlaps with Phase 05, but the concept is valid).

### Block 6 — Reproducibility and debuggability
- **Fit: Full.** The concepts (what varies per run and is recorded; unbounded
  resources; order-independence as a per-test property; process-wide state shared
  with production code) apply directly. The project uses `tmp_path` for isolation,
  `monkeypatch` for overrides, and `datetime.now()` / `time.time()` in
  `AppService` timing — all shared-clock concerns.
- **Outdated:** L83: "A shuffling plugin makes no individual test independent" —
  no shuffling plugin (`pytest-randomly`, `pytest-random-order`) is installed or
  configured. The *principle* is correct (order can create false independence),
  but the reference to a specific plugin type is misleading.
- **Replacement:** Reword "A shuffling plugin makes no individual test
  independent" → "Reordering tests does not make an order-dependent test
  independent." This preserves the principle without presuming a specific tool.

### Block 7 — What actually blocks a merge
- **Fit: Full.** The concept (for each declared gate, establish whether it is
  loaded, green, and in-scope) is framework-agnostic and directly applicable.
- **Outdated:** L90 lists gate types — "security and dependency and secret
  scanning" — that are inherently absent for this project: local-only, no
  network, no credentials, no deployment, no CI/CD pipeline. "Deploy check" and
  "scheduled suites" are likewise inapplicable. Phase 12 (Production Operations,
  Security & Observability) owns the security/observability surface, but Phase 12
  is async-framed and also needs re-basing; security scanning is not a Phase 11
  concern regardless.

  The gate list on L90 reads as an assumption that these gates *should* exist,
  not an enumeration to check. For this project, the effective gate set is:
  ruff (static analysis), mypy (types), pytest (tests). No coverage gate,
  no translation-completeness gate (no i18n infra yet — but project rules §16
  say i18n is part of DoD, so this is a future gate), no secret/dependency
  scanning (no secrets to scan, no deployment to protect).

- **Replacement:** Reword the gate list on L90 to be project-aware. Replace
  "security and dependency and secret scanning, deploy check, scheduled suites"
  with a framing that makes these conditional: "any other gates declared in the
  project's configuration (e.g., translation completeness per project rules §16;
  security or dependency scanning where the project has network or deployment
  surfaces — this project does not)." This keeps the block framework-agnostic
  while not implying gates that cannot exist.

  Separately, the Block 7 severity examples in the taxonomy (L117-122) are
  framework-agnostic and need no changes (see §6).

### Block 8 — Declared test-layer contracts
- **Fit: Full.** The concept (every property the test layer asserts about itself;
  whether anything checks it; a comment that contradicts the executing path)
  applies directly. The project has docstrings in `conftest.py` (e.g.,
  `sample_dictionary` docstring at `tests/conftest.py:54`), the `slow` marker
  description, and config-file assertions (`test_app.py:207-221` asserts
  `yaml_to_dict` returns `{}` on missing/invalid YAML — see Phase 10 F5/QLT-001).
- **Outdated:** None.
- **Replacement:** No changes.

---

## 3. Purpose & Scope Boundary Assessment

### Scope boundary (line 18)

Current text:
> Scope boundaries — other phases own: production-code convention defects and dead
> code (10); pipeline and container posture (12); transaction, lock, and pooler
> semantics (03); lifecycle correctness (05); settings values and environment
> policy (02); namespace rulings and cross-phase conflict resolution (99).

| Cited phase | Current citation text | Status | Action | Rationale |
|---|---|---|---|---|
| (10) | "production-code convention defects and dead code" | Re-based, exists | **KEEP** | Phase 10 (Code Quality) is re-based and owns conventions/dead code. Valid deferral. |
| (12) | "pipeline and container posture" | Re-based, exists (async-framed) | **KEEP (reword)** | "Pipeline posture" is valid (operational posture, packaging). But **"container"** is invalid — the project has no containers (Phase 12 purpose L17: "no containers"). Reword to "pipeline and operational posture." |
| (03) | "transaction, lock, and pooler semantics" | **Deleted** | **REMOVE** | Phase 03 ("Async Concurrency & Worker Safety") deleted per Phase 03 auditor report §5 and Phase 03 researcher recommendations §2. "Transaction, lock, pooler" was the async Semaphore/Queue/worker-pool model. SQLite transaction/locking semantics belong to Phase 04 (pipeline/DB layer). |
| (05) | "lifecycle correctness" | Re-based, exists | **REWORD → (01)** | Phase 05's actual purpose is "Config Template Validation" — it does not own "lifecycle correctness." Per Phase 10 researcher recommendations §6.1 and Phase 10 auditor report §3, "lifecycle correctness" is now Phase 01's domain ("import-time bootstrap and the settings gate"). Reword to cite (01). |
| (02) | "settings values and environment policy" | Re-based, exists | **KEEP** | Phase 02 (Configuration Management) owns settings values. Valid deferral — test fixtures consume settings (`realistic_config` in `conftest.py`). |
| (99) | "namespace rulings and cross-phase conflict resolution" | Exists | **KEEP** | Phase 99 (Validation) owns cross-phase ownership and namespace integrity. Valid. |
| (16) | (line 20: "rendered outcome") | Exists | **KEEP** | Phase 16 (Rendered Surface) owns output conformance. Phase 11 owns "whether the suite would notice" a rendered output defect. Correct. |
| (17) | (line 20: "derived quantity equals") | Exists | **KEEP** | Phase 17 (Arithmetic) owns derived-quantity correctness. Phase 11 owns "whether the suite would notice" an arithmetic defect. Correct. |

**Recommended rework of line 18:**
> Scope boundaries — other phases own: production-code convention defects and dead code
> (10); pipeline and operational posture (12); import-time bootstrap and the settings
> gate (01); settings values and environment policy (02); namespace rulings and
> cross-phase conflict resolution (99).

(Remove "(03) transaction, lock, and pooler semantics"; reword "(05) lifecycle
correctness" → "(01) import-time bootstrap and the settings gate"; reword "(12)
pipeline and container posture" → "(12) pipeline and operational posture".)

**Missing citation — (04):** Phase 11's Block 5 (fixture setup involving `DBWorker`
context manager, SQLite lifecycle) and Block 3 (fixture data contract for DB
tables) overlap with Phase 04 (Data Pipeline & Report Lifecycle). Phase 04's scope
excludes only (02) and (13) — it does not explicitly exclude (11). Adding "(04)
for DB lifecycle and pipeline teardown in fixtures" would be a thorough addition,
but it is **optional** — Phase 10's researcher recommendations took the same
minimalist approach (cite only the major deferrals). **Recommendation: add (04)**
for completeness of the test-harness ↔ pipeline-lifecycle handoff.

---

## 4. Block 7 "Secret Scanning" — Detailed Assessment

Phase 11 Block 7 (L90) enumerates gate types:

> "For each declared gate — static analysis, types, tests, coverage, translation
> completeness, **security and dependency and secret scanning**, deploy check,
> scheduled suites — establish whether it is loaded, whether it is green, and what
> its scope excludes."

**What gates actually exist** (verified against `pyproject.toml` + repo):

| Gate type | Declared? | Loaded? | Green? |
|---|---|---|---|
| Static analysis (ruff) | Yes — `[tool.ruff.lint]` L111 | Yes — `ruff check` runs | Unknown (not in CI — no `.github/`) |
| Types (mypy) | Yes — `[tool.mypy]` L140 | Yes — `mypy` configured | Unknown (not in CI) |
| Tests (pytest) | Yes — `[tool.pytest.ini_options]` L152 | Yes | Yes (tests pass) |
| Coverage | No | No | No |
| Translation completeness | No (no i18n infra) | No | — |
| Security/dependency/secret scanning | No | No | — |
| Deploy check | No (no deployment) | No | — |
| Scheduled suites | No (no CI) | No | — |

The mention of "security and dependency and secret scanning" as a gate type is
**not wrong** per se — the block asks the auditor to *check* whether each gate is
loaded. The auditor would establish: "not loaded, not applicable — no network,
no credentials, no deployment surface." However, the phrasing reads as an
**assumption** that these gates should exist, rather than a neutral enumeration.
For a local-only CLI tool, listing "secret scanning" alongside "static analysis
and types and tests" implies a parity that does not exist and cannot exist
(there are no secrets in a no-network, no-credentials tool).

**Recommendation ([BEST-PRACTICE]):** Reword the gate list on L90 to make
inapplicable categories explicitly conditional, following the Phase 12 precedent
("There is no edge tier, no transport security boundary" — Phase 12 L17). The
block should note that for this project, several gate categories are **inherently
absent by design**, and that security/observability gates (where applicable) belong
to Phase 12's domain, not Phase 11's.

---

## 5. The `TST-` Prefix Collision Warning — VERIFICATION

Phase 11 line 128 states:

> Finding-ID prefix: `TST-` — **already in use in shipped source and operations
> documentation from a prior cycle, with at least one identifier already resolving
> to two different findings; check for a collision before minting an ID and report
> it rather than creating a second namespace.**

**This claim is disproven by the working tree (code_ref `384044a`):**

A repo-wide grep for `TST-` (literal, across `.py`, `.md`, `.yaml`, `.toml`,
`.txt`, `.env*`):

- **2 matches total**, both in the Phase 11 phase file itself:
  - `.kilo/commands/audit/phases/11-audit-test-coverage.md` L128 (the warning)
  - `.ai/tmp/scratch/.kilo/commands/audit/phases/11-audit-test-coverage.md` L128
    (a non-tracked scratch copy — outside `git ls-files`)
- **0 matches** in any shipped source file (`.py`, `.yaml`, `.toml`)
- **0 matches** in any existing findings document or report
- **0 matches** in `.ai/reports/` (Phase 01–10, 03, 05, 09, 15 auditor reports)
- **0 matches** in `.ai/audit/` (only `templates/audit-findings.md` and
  `problems/01-apply-delete-double-execution.md` exist — neither contains `TST-`)
- **No findings directory `.ai/audit/11-test-coverage/`** exists (`Test-Path` →
  `False`; `glob .ai/audit/**` shows only `templates/` and `problems/`)

The `audit-findings.md` template (`.ai/audit/templates/audit-findings.md`) uses
`QLT-` placeholders (from a different project's Django ads app — Phase 10 auditor
report §6.1), **not** `TST-`. There are no `TST-` placeholders anywhere.

**Conclusion:** There is no "prior cycle" `TST-` usage in shipped source or
operations documentation. The Phase 11 phase file's warning is **stale phantom
evidence**, identical in nature to the `QLT-` and `OPS-` and `PERF-` warnings in
Phases 10/12/13 (Phase 10 disproved its own `QLT-` warning; Phase 13 makes the
same unsupported claim for `PERF-`).

**Recommendation:** Retain the `TST-` prefix (it is unique — zero collisions in
the current tree, consistent with the template convention). **Remove the "already
in use" warning** on line 128. Replace with corrected text stating that `TST-` is
the Phase 11 prefix per the template convention and that no collision was found in
the current tree. This mirrors Phase 10's resolution (Phase 10 auditor report §6.1,
Phase 10 researcher recommendations §5.1).

---

## 6. Severity Taxonomy Assessment

**Verdict: Keep — minor rewording.**

Phase 11's severity taxonomy (lines 112–122) opens with the **sound principle**
shared across all phases: "Grade by effect and blast radius, not by mechanism name;
rate what is true now, not the worst consequence if triggered. The bands are effect
classes, not a closed list of mechanisms."

**Assessment of each band's examples:**

- **CRITICAL** — "the suite's green is affirmatively false today: a shipped assertion
  the product cannot satisfy, or a decision the system acts on whose effect no assertion
  anywhere can observe, where the unobserved effect is one **a user or a seller receives**."
  → Framework-agnostic except for **"seller"** (L116). "Seller" is advertising-domain
  jargon from the Mediascope context. The actual project post-processes advertising
  exports; the relevant audience is the **data engineer / analyst / operator** who
  consumes the output, not a "seller." Reword: "a user or an operator receives."
  One-word rewording.

- **HIGH** — "a gap that will be mistaken for coverage and is silent today: a decision
  exercised only through a stand-in for the thing it decides, a boundary asserted from
  one side only, a property the harness depends on that nothing holds it to, a gate
  declared and never loaded, a measurement whose effective scope is not the scope it is
  read for."
  → **Framework-agnostic.** No rewording needed. All examples apply: "a gate declared
  and never loaded" directly maps to the absent coverage/security/CI gates; "a measurement
  whose effective scope is not the scope it is read for" applies to any test coverage
  metric.

- **MEDIUM** — "bounded and real, with a specific audience: a gate loaded and red over a
  known accepted-noise class, a resource left unbounded so that one test can take the run
  down, state carried forward between runs, a cost that makes a gate get disabled."
  → **Framework-agnostic.** No rewording needed. "A resource left unbounded" applies to
  SQLite temp files / `tmp_path`; "state carried forward between runs" applies to temp
  DB files if not cleaned.

- **LOW** — "self-description and hygiene with no runtime consequence today: a test-layer
  comment that misdescribes the harness, a duplicated helper with no divergence yet, a
  marker registered and inert, a test whose subject is configuration text it cannot
  actually detect a change in."
  → **Framework-agnostic.** No rewording needed. "A marker registered and inert" is
  **directly applicable** — the `slow` marker is registered in `pyproject.toml:167`
  but never used in any test file (grep: 0 matches for `@pytest.mark` in `tests/*.py`).

**Note on format:** Phase 11 uses prose list format for the taxonomy (L115-122),
while Phase 02 uses a table format (L111-116). This is a **pre-existing inconsistency**
across phases, not introduced by Phase 11. The prose format is readable and consistent
with Phases 05, 10, 13, and 99. No change required, but Phase 99 §8 should eventually
standardize the template format.

**Recommendation:** Reword "seller" → "operator" on L116. Keep all other taxonomy
text verbatim.

---

## 7. Report Output Assessment

| Item | Current (Phase 11 §Report Output) | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/11-test-coverage/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 02: `.ai/audit/02-config/`, Phase 05: `.ai/audit/05-config-templates/`, Phase 10: `.ai/audit/10-code-quality/`). Directory does not yet exist — no prior findings to reconcile. |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template; front matter defines `severity_taxonomy`, `code_ref`, `id_prefix` fields. Verified present at `.ai/audit/templates/audit-findings.md`. |
| ID prefix | `TST-` | **Keep (correction required)** | See §5. Retain `TST-`; remove the stale "already in use" collision warning. |
| Incremental append | ≤100 lines per pass | **Keep** | Template constraint. Applies. |
| problems-only | `true` (front matter L6 + §L131) | **Keep** | All findings will be code-verified defects. |
| Empty state | `No problems found in this phase.` | **Keep** | Verbatim. |
| Shipped-test-as-blocker | L133: "a shipped test asserting current behaviour is a remediation blocker" | **Keep** | Directly applicable — `test_app.py:207-221` asserts `yaml_to_dict` returns `{}` on missing/invalid YAML (Phase 10 F5/QLT-001), which masks config errors. A test-layer finding relying on this behavior is a remediation blocker. |

**Template note ([DOC-UPDATE]):** The `audit-findings.md` template
(L48–50, L54, L72, L81) ships with `QLT-001`/`QLT-002` placeholder examples using
Django `src/backend/apps/ads/` paths from a different project. These are scaffold
defaults, not real findings in this repo (Phase 10 auditor report §6.1). When Phase
11 initializes its findings file, it should replace these with `TST-` placeholders
or leave them as generic examples. This is a pre-existing template issue, not
specific to Phase 11, but flagged for awareness.

---

## 8. Cross-Phase Citations of (11) — Who References Phase 11

| Phase | Line | Citation text | Phase status | Action on (11) citation |
|---|---|---|---|---|
| 04 | — | (not explicitly cited in scope, see note) | Re-based (sync) | N/A — Phase 04 doesn't cite (11) in its scope boundary, but Phase 13 and Phase 12 do |
| 12 | 25 | "test-suite adequacy (11)" | Async-framed, to be re-based | **KEEP** — valid concern (test gates, CI). Verify when 12 is re-based. |
| 13 | 27 | "test-suite adequacy (11)" | Async-framed, to be re-based | **KEEP** — valid (coverage measurement scope). |
| 16 | 27 | "test-suite adequacy (11)" | Async-framed | **KEEP** — valid (does the suite catch output-conformance defects?). |
| 17 | 28 | "test-suite adequacy (11)" | Async-framed | **KEEP** — valid (does the suite catch arithmetic defects?). |

All downstream phases that cite (11) reference a valid, real concern. No citation
cleanup is needed in Phase 11's referencing phases.

---

## 9. Conclusion

**Verdict: REBASE — minor-to-moderate rework.**

Phase 11's eight test-coverage blocks are **framework-agnostic** principles that
apply fully to the synchronous, pytest-based test suite. Unlike Phase 03 (async
concurrency — deleted) or Phase 09 (external API — deleted), Phase 11 has:

- **Zero blocks to delete** — every block's core concept is valid
- **Six blocks (1, 2, 3, 4, 6, 8) that are Fit: Full** with no or trivial outdated
  references
- **Two blocks (5, 7) that are Fit: Partial** — their principles are valid, but
  specific phrases assume infrastructure that does not exist (two execution paths,
  fast-path exclusion, import-path shims, secret/dependency scanning gates)

The required rework is **targeted rewording only**, following the exact precedent
set by Phase 10 (which also concluded REBASE with minor rewording):

1. **Line 18 (scope boundary):** Remove the stale citation to deleted Phase 03
   ("transaction, lock, and pooler semantics"); reword "(05) lifecycle correctness"
   → "(01) import-time bootstrap and the settings gate"; reword "(12) pipeline and
   container posture" → "(12) pipeline and operational posture" (no containers).
   Optionally add "(04)" for DB-lifecycle-in-fixtures handoff.

2. **Block 5 (L70-75):** Reword five presumptive phrases
   ("parallel execution", "two execution paths this project supports", "fast path",
   "two helper universes", "import-path shim") to conditional checks.

3. **Block 6 (L83):** Reword "a shuffling plugin makes no individual test
   independent" → "Reordering tests does not make an order-dependent test
   independent."

4. **Block 7 (L90):** Reword the gate list to make inapplicable categories
   (secret/dependency scanning, deploy check, scheduled suites) explicitly
   conditional for a local-only, no-network CLI tool.

5. **Block 2 (L43):** Reword "scheduling primitive" → "ordering mechanism."

6. **Severity taxonomy (L116):** Reword "seller" → "operator."

7. **Report Output (L128):** Remove the stale "TST- already in use" collision
   warning — disproven by grep (zero matches outside the Phase 11 file itself and
   a non-tracked scratch copy). Retain the `TST-` prefix.

**Effort: small** (≤8 line edits to the phase file; no source files modified).
**Priority: recommended** — the stale citations to deleted Phase 03 in the scope
boundary are actively misleading to any auditor executing this phase; the TST-
prefix warning is phantom evidence that could cause an auditor to avoid minting
`TST-` IDs unnecessarily.

No source code, test files, or findings documents were modified by this report.
