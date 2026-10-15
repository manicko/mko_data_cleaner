---
phase: "03"
phase_name: "Async Concurrency & Worker Safety"
report_status: "final"
recommendation: DELETE
---

# Researcher Recommendations — Phase 03 Async Concurrency & Worker Safety

## 1. Proposed Final Block Set

**DELETE Phase 03 entirely. No re-basing. All 5 blocks are rejected.**

The auditor's report (`.ai/reports/phase-03-auditor-report.md`) assessed every block under
**Fit / Outdated / Surviving concept**:

| Block | Title | Fit | Outcome |
|---|---|---|---|
| 1 | Concurrency ceiling and its interaction with the pool | **None** | **delete** — no Semaphore, no pool, no queue |
| 2 | Worker cancellation and shutdown discipline | **None** | **delete** — redundant with Phase 01 Block 6 (`DBWorker` teardown) |
| 3 | Backpressure through the asyncio.Queue | **None** | **delete** — redundant with Phase 13 Block 5 (memory pressure) |
| 4 | Error isolation within the worker fan-out | **None** | **delete** — no fan-out; belongs to Phase 04 (error serialisation) |
| 5 | Retry isolation inside the concurrency boundary | **None** | **delete** — no retries; belongs to Phase 09 (async-framed, no subject) |

All five blocks are **Fit: None, Outdated: entirely**. No concept survives a re-base onto
the synchronous single-shot surface — the only viable analog ("worker lifecycle and clean
shutdown") is already Phase 01 Block 6. Re-basing would duplicate, not extend, coverage.

**Confidence: HIGH** — grep-verified: zero matches for `asyncio`, `async def`, `Semaphore`,
and `Queue` across `src/mko_data_cleaner/`. The architecture is a straight `app →
AppService.run_report → CSVWorker → DBWorker` chain, one OS process per invocation.

## 2. Cross-Phase Citation Cleanup

Phase 03 is cited as out-of-scope by six downstream **phase files** in
`.kilo/commands/audit/phases/`. Every citation is a vestigial reference to a non-existent
Semaphore/Queue/worker-pool/cancellation model and must be removed. The auditor report §3
already enumerated these; confirmed verbatim against the working tree:

| File (tracked) | Line | Exact current text | Action |
|---|---|---|---|
| `phases/04-audit-data-pipeline.md` | 27 | `Owned elsewhere: the Semaphore and Queue concurrency (03); the retry and` / line 28: `backoff contract (09);` | remove the `(03)` clause; the line becomes `Owned elsewhere: the retry and` |
| `phases/09-audit-external-api.md` | 23–24 | `Semaphore/Queue/worker-pool` / `semantics and the cancellation mechanism (03);` | remove the two `(03)`-tagged clauses spanning lines 23–24 |
| `phases/10-audit-code-quality.md` | 17 | `transaction, lock, and pooler semantics (03);` | remove this clause (transaction/lock semantics belong to the DB layer owned by 04, per auditor §3 note) |
| `phases/11-audit-test-coverage.md` | 18 | `transaction, lock, and pooler semantics (03);` | remove this clause (same re-targeting as 10) |
| `phases/12-audit-production-ops.md` | 23 | `Semaphore/Queue/worker-pool concurrency (03);` | remove this clause |
| `phases/13-audit-performance.md` | 25 | `acquisition and cancellation semantics (03);` | remove this clause |

**Two self-describing auditor/researcher reports already recommend the same removal** and
need no mechanical edit — they *are* the recommendation record:

- `.ai/report/phase-01-auditor-report.md` line 201: *"defers Semaphore/Queue to phase 03,
  which is itself moot here"* — this **is** the Phase 01 §3 recommendation to drop the
  exclusion (already recorded, see Phase 01 auditor report §2 Block 6).
- `.ai/report/phase-01-auditor-report.md` lines 220–221 and 306–307: recommendation to
  *delete* the `(03)`/`(09)` scope exclusions from Phase 01's Purpose — already in force.
- `.ai/reports/phase-01-researcher-recommendations.md` line 72: *(Delete the
  async/Semaphore/Queue (03), blocking-work-bridge (09), and "credential provenance (02)"
  exclusions …)* — already applied to Phase 01.
- `.ai/reports/phase-01-researcher-recommendations.md` §4.5: cross-phase consistency risk
  flagging Phase 03 as async-framed → now resolved by this deletion.

**Excluded from cleanup:** `.ai/tmp/scratch/.kilo/commands/audit/phases/*` — a scratch copy
outside the tracked tree (not `git ls-files` tracked); left untouched.

## 3. Recommended Modern Practices / Angles

The async-concurrency analysis angles in Phase 03 are **inapplicable** to this codebase and
there is no synchronous re-framing that would make them valid:

- **Semaphore-bounded concurrency ceiling** → no concurrency exists; the synchronous chain
  has no concurrency boundary to bound. The resource-lifecycle concern lives in Phase 01
  Block 6 (`DBWorker`: checkpoint WAL + delete on `__exit__`).
- **Worker cancellation / shutdown discipline** → no workers, no `cancel()`, no event loop.
  Clean-teardown is already owned by Phase 01 Block 6.
- **Queue backpressure** → no `Queue(maxsize=)`, no producer coroutine. Bounded work
  generation survives only as a *memory/streaming* property owned by Phase 13 Block 5.
- **Fan-out error isolation** → no fan-out. Per-task error capture and re-run-ability is
  Phase 04's domain ("Error serialisation and task-to-yaml").
- **Retry isolation with backoff + jitter** → no retry mechanism (no network, no transient
  failures). Retry/backoff contract was delegated to Phase 09, which itself is entirely
  async-framed (`asyncio.to_thread`) and has no subject here — a separate deletion candidate.

**No new practice should be grafted onto Phase 03.** The synchronous equivalent of every
surviving concern (resource lifecycle, import-time side effects) is already covered by the
re-written Phase 01.

## 4. Quality / Brevity Concerns

Deletion is clean and has **no numbering or path-drift impact**:

- The findings output path `.ai/audit/03-async-concurrency/findings.md` and Finding-ID prefix
  `ASY-` (declared at Phase 03 line 118 / 122) are **void** — the directory
  `.ai/audit/03-async-concurrency/` does not exist (verified: `Test-Path` → False), so no
  artifacts need migrating or invalidating.
- Phase 03 has `status: draft` and `validated: no` (front matter lines 3–4); it has produced
  no findings and is selected only via directory enumeration in `audit-multi-phase.md`
  (grep confirms no hard-coded `03` sequence there), so deleting the file removes it from the
  selectable set automatically.
- No downstream phase hard-codes an ordered sequence that would shift when Phase 03 is
  removed — phase references are by `(NN)` citation, not numeric index.
- The `.ai/tmp/scratch/` mirror is a non-tracked copy (scratch recipe artifact) and is
  correctly excluded from any edit scope.

## 5. Edit Delta

**Net change — two operations:**

1. **Delete the file.**
   - Remove `.kilo/commands/audit/phases/03-audit-async-concurrency.md` (126 lines,
     `status: draft`). No replacement file. The `ASY-` prefix and
     `.ai/audit/03-async-concurrency/findings.md` path die with it.

2. **Remove the six `(03)` citations** listed in §2 from the downstream phase files
   (Phases 04, 09, 10, 11, 12, 13). The exact text to remove per file is enumerated in the
   §2 table; each is a scoped clause deletion in the "Owned elsewhere" / "Scope boundary"
   prose, with no renumbering of blocks in the citing phase.

**Out of scope for this delta — tracked as follow-on only:** the broader async re-basing of
Phases 04, 09, 12, 13 (and 15), whose Purposes still assume a standalone async CLI tool and
Mediascope API fetches. Those carry the same async fiction and need their own rewrite
(mirror the Phase 01/02 pattern: strip async/Mediascope-API framing, delete
credential/async-specific blocks, re-anchor surviving concepts on the synchronous surface).
The `(03)` citation removal above is the only edit Phase 03's deletion *requires*; the
async re-basing of the citing phases is independent work.

**Effort: trivial** (one file deletion + ≤6 line-edits). **Priority: recommended** — Phase 03
is draft and findings-path-less, but removal is safe and prevents auditors from chasing a
phantom async model.
