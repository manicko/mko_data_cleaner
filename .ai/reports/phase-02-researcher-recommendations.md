---
phase: "02"
phase_name: "Configuration Management"
phase_file: ".kilo/commands/audit/phases/02-audit-config-secrets.md"
source: ".ai/reports/phase-02-auditor-report.md"
status: "recommendations"
---

# Phase 02 — Researcher Recommendations

## 1. Executive Summary

The current Phase 02 file (`02-audit-config-secrets.md`) was authored for a
**credential-bearing, async, network-calling** tool. The actual codebase is a
**synchronous, single-shot, local-only** CLI tool and library — no network,
no credentials, no async. Every section that references credentials, the
Mediascope API, async pipelines, or environment-variable overrides must be
stripped or rewritten. Block 2 ("Credential seam") has no target at all and
must be deleted. The remaining six blocks are retained but retargeted to
real codebase properties: flat YAML config loading, existence-only
settings gate, `yaml_to_dict` `{}` fallback on parse error, `settings_dir`
forwarding asymmetry (D-6), dead `merge_dicts` helper, dead dependencies,
doc-code drift on `log_config.yaml` logger names, missing `__main__.py`,
and a stale dev-only execution block in `app_service.py`.

## 2. Proposed Final Block Set

| # | Block (current title) | Action | Rationale |
|---|---|---|---|
| 1 | **Block 1**: Settings surface & template distribution topology | **MODIFY** | Valid core concept; remove phantom "defaults directory"; add `settings_dir` forwarding asymmetry |
| 2 | **Block 2**: Credential seam: where the Mediascope secret enters | **DELETE** | No credentials exist; no network; no `connections/` directory |
| 3 | **Block 3**: Pydantic validation fidelity & defaults merge | **MODIFY** | Remove "defaults merge" framing (no overlay exists); add malformed-YAML passthrough; flag `merge_dicts` dead code |
| 4 | **Block 4**: Secret validation & fail-fast behaviour | **REWRITE** | Rename to "Settings gate: fail-fast fidelity"; drop all secret/credential framing |
| 5 | **Block 5**: Dead, ineffective & unconsumed configuration | **KEEP + ENRICH** | Best-fitting block; add dead-code findings (merge_dicts, dead deps, doc drift, stale dev block) |
| 6 | **Block 6**: Environment separation & process parity | **REWRITE** | Drop credential exposure/debug logging; focus on entry-surface parity, `__main__.py` gap, `settings_dir` leakage |
| 7 | **Block 7**: Env-overridable production posture | **REWRITE** | Drop concurrency/retry/credential posture; focus on real config-driven posture (db_file, reader/writer options) |

**No blocks to ADD or MERGE.** All overlaps cited by the auditor are mild
and acceptable — blocks remain functionally independent. Block 2 is deleted
and remaining blocks retain their original numbers (3→3, 4→4, etc.), producing
the sequence 1, 3, 4, 5, 6, 7.

## 3. Section Rewrites Beyond Blocks

### 3.1 Purpose (L13–19) — REWRITE

Strip "async," "Mediascope API fetch," "credentials," and "connections/"
from the purpose text. Replace with:

> Audit how configuration is declared, resolved, validated, and consumed in
> a synchronous, single-shot, local-only CLI tool and library that
> post-processes Mediascope TV advertising exports received as files on disk
> (no network, no credentials, no authentication). Bundled YAML templates live
> in the package `settings/` tree and are copied to the user settings directory
> by the `init` command. All loaded configuration is validated through
> Pydantic v2 models before use.

### 3.2 Scope / Inter-phase Ownership (L21–24) — REWRITE

Remove (09) blocking-work bridge, (03/04) async pipeline, (02) credential
provenance exclusions. Replace "merged" with "consumed." Add explicit
in-scope items:

> Other phases own: process startup and import-time behaviour (01);
> fixed-value enum discipline (10); packaging and CI/CD (12). This phase owns
> the **values** themselves — how they are sourced, validated, and consumed —
> including the `settings_dir` forwarding asymmetry between `init` and `run`
> surfaces and the malformed-YAML passthrough defect.

### 3.3 Severity Taxonomy (L119–124) — REWRITE

Rebase all examples on the real runtime (no credentials/secrets/async):

| Band | Rewritten Conditions |
|---|---|
| CRITICAL | Config gate bypassed or silently disabled — reversion to silent fallback on missing settings. **Empty today** (do not regress): `_require_settings` does raise. |
| HIGH | Malformed-but-present config passes the gate and surfaces as an opaque downstream error; an unvalidated config value reaches a consumer causing silent data corruption. |
| MEDIUM | Dead or unconsumed configuration (e.g., a helper with zero call sites); declared dependencies never imported; setting-name drift; leakage of config across entry surfaces. |
| LOW | Stale dev-only execution blocks in production modules; non-atomic template copy; missing `__main__.py` so the bare package entry fails; doc-code divergence on config/logger names. |

### 3.4 Report Output (L133) — MODIFY

Findings path: `.ai/audit/02-config-secrets/findings.md` →
`.ai/audit/02-config/findings.md` (strips "secrets" from the directory name).

## 4. Block Formulations (≤6 lines each)

Each formulation is self-contained, checklist-style, and guides *how to look*
without prescribing files, modules, or procedures.

### 1. Settings surface & template distribution topology

```
### 1. Settings surface & template distribution topology

- Which config variant does each running surface consume, and via what reader?
- Are package templates copied to the user directory preserving edits, with an atomic write on re-init?
- Does init honor the same settings_dir that run honors, or does one ignore a caller override?
Evidence: effective variant per surface; copy mechanism; one preserved-or-clobbered file.
```

### 3. Pydantic validation fidelity & config loading

```
### 3. Pydantic validation fidelity & config loading

- Is each YAML loaded directly (no overlay merge) and validated through a typed model before use?
- Does a missing required key fail fast with a named field on the validated model?
- Does a present-but-malformed file surface a cause-specific error at the gate, or does the loader return a blank that masks the syntax-failure cause?
Evidence: load+validate chain; one induced parse error and its message; the validated model.
```

### 4. Settings gate: fail-fast fidelity

```
### 4. Settings gate: fail-fast fidelity

- Is the config existence gate unconditional and fail-fast on missing settings files?
- Does the gate catch malformed-but-present files, or only absence — and where does the malformed path surface instead?
- Is the failure actionable (names the file, points to remediation) and consistent across all entry surfaces?
Evidence: gate inventory with scoping verdict; one observation per gate; the failure text.
```

### 5. Dead, ineffective & unconsumed configuration

```
### 5. Dead, ineffective & unconsumed configuration

- Is each assigned setting name still read by consumer code and every declared value reaching a reachable consumer?
- Are declared dependencies in the project manifest actually imported anywhere in source?
- Does any documented config key or logger exist in docs but not in the shipped file?
Evidence: per-name liveness verdicts; unread-name list; derived-value validity check.
```

### 6. Entry-surface parity & config forwarding

```
### 6. Entry-surface parity & config forwarding

- Do the CLI, library API, and module entry load the same config and honor the same settings_dir identically?
- Does the bare package entry (python -m <package>) launch the CLI, or only the submodule form?
- Can a value for one surface be ignored by another due to divergent settings_dir handling?
Evidence: effective-config diff per entry surface; the honoured config paths per surface.
```

### 7. Config-driven operational posture

```
### 7. Config-driven operational posture

- Which settings change operational posture (output shape, format, DB location) rather than just defaults?
- Are posture-affecting settings env-overridable, or is there no env-override layer at all?
- Does the DB file location isolate per report, or live inside the report directory by default?
Evidence: the list of posture-affecting settings with the pinning status of each.
```

## 5. Recommended Modern Practices / Analysis Angles

1. **Boundary validation (fail-fast)** — Validate config *content* (not just
   file existence) at the entry point. The current existence-only gate lets
   malformed YAML through; modern practice gates on parseability *and*
   presence simultaneously.

2. **Exception chaining for diagnostic fidelity** — When wrapping a YAML parse
   error as a `ConfigError`, use Python's `raise ... from original` to preserve
   the root cause. The current `yaml_to_dict` returning `{}` on error discards
   the syntax detail entirely — the user sees a Pydantic "field required" error
   instead of "your YAML is invalid."

3. **Atomic file operations for template initialization** — When copying
   package templates to the user directory, write to a temporary file then
   rename (atomic on POSIX). The current file-by-file copy
   (`_copy_templates`) can leave a partial state visible to concurrent readers.

4. **Dependency hygiene** — Regularly prune unused dependencies with `uv
   prune` or equivalent. Declared-but-unimported packages (`pandas`,
   `pydantic-settings`, `python-dateutil`, `urllib3`) confuse readers,
   inflate the attack surface, and mislead security scanners.

5. **Single-source-of-truth for logger/config names** — The config file's
   logger names should be documented in exactly one place. The current
   doc-code drift (`tests`/`telebot` in docs vs. only `__main__` in the
   shipped file) is a maintenance hazard.

6. **Standard Python entry-point conventions** — If `python -m <package>` is
   a documented entry point, the package must ship a `__main__.py`. The
   current absence means the bare package entry silently fails — a violation
   of PEP 366 / PEP 338 conventions.

7. **Configuration-as-typed-model** — Continue using Pydantic v2 models as
   the validation boundary (already in place). The gap is that validation is
   deferred to cached_property access rather than performed eagerly at load
   time. Moving validation to load time would make the fail-fast gate
   comprehensive.

## 6. Remaining Quality & Brevity Concerns

1. **File name drift (HIGH)**. The phase file is named
   `02-audit-config-secrets.md` and frontmatter `name: 02-config-secrets`.
   With secrets removed, both should become `02-config`. This ripples to the
   findings output path (L133). A rename requires checking the audit
   runner's phase-discovery logic for hard-coded references. **Recommended**:
   rename to `02-audit-config.md` / `name: 02-config` /
   `02-config/findings.md`.

2. **Phase title (HIGH)**. Line 9: `# Phase 02 — Configuration & Secrets
   Management` contains "Secrets." Should become `# Phase 02 — Configuration
   Management`.

3. **Non-contiguous numbering (MEDIUM)**. Deleting Block 2 creates a gap
   (1, 3, 4, 5, 6, 7). The task explicitly requests this (3→3, 4→4), but
   some executors may expect contiguous numbering. **Recommended**: add a
   one-line note after the blocks: `> Note: Block 2 was deleted — no
   credentials exist in this codebase.`

4. **Frontmatter `executor` field (LOW)**. Currently `executor: auditor`.
   This is correct (the phase is executed *by* auditors) and needs no change.
   Do not alter it to "researcher."

5. **Audit-intro note (L30–33) (LOW)**. The "Each block is independent" note
   and "Evidence is a class, not a check list" convention are generic and
   remain valid. No change needed.

6. **Over-exported `__all__` (LOW, ADJACENT)**. `app.py:134-142` exports
   Typer internals alongside contract functions; package `__init__.py`
   exposes only `__version__`. This is an API-surface issue, not config.
   It can be filed as a LOW finding under Block 5 or Block 6 but does not
   warrant a dedicated block.

7. **Evidence-line brevity (LOW)**. Each rewritten block includes a one-line
   Evidence summary. These are already ≤1 line. Ensure the doc-specialist
   preserves this constraint when finalizing — do not let Evidence lines
   expand to multi-line prose.

## 7. Edit Delta Table (for doc-specialist)

| # | Section | Current L | Action | Replacement / Notes |
|---|---|---|---|---|
| 1 | Frontmatter `name` | L2 | Modify | `02-config-secrets` → `02-config` |
| 2 | Title | L9 | Modify | "Configuration & **Secrets** Management" → "Configuration Management" |
| 3 | Purpose text | L13–19 | Rewrite | Strip "async", "Mediascope API fetch", "credentials", "connections/". See §3.1. |
| 4 | Scope / inter-phase | L21–24 | Rewrite | Remove (09), (03/04), (02) exclusions. Replace "merged" with "consumed". Add in-scope: settings_dir asymmetry + malformed-YAML passthrough. See §3.2. |
| 5 | Block 1 heading | L35 | Modify | Keep: "Settings surface & template distribution topology" |
| 6 | Block 1 content | L37–45 | Modify | Remove "defaults directory" phantom; remove multi-surface "loads" framing; add settings_dir forwarding asymmetry. See §4, Block 1. |
| 7 | Block 2 (entire) | L47–57 | Delete | "Credential seam: where the Mediascope secret enters" — no credentials exist. |
| 8 | Block 2→3 gap | L58 | Delete | Remove blank line between Block 2 and Block 3. |
| 9 | Block 3 heading | L59 | Modify | "Pydantic validation fidelity & **defaults merge**" → "Pydantic validation fidelity & config loading" |
| 10 | Block 3 content | L61–69 | Rewrite | Remove "defaults-overlay merge" framing; add malformed-YAML passthrough as primary validation-fidelity defect; flag `merge_dicts` dead code. See §4, Block 3. |
| 11 | Block 4 heading | L71 | Modify | "Secret validation & fail-fast behaviour" → "Settings gate: fail-fast fidelity" |
| 12 | Block 4 content | L73–81 | Rewrite | Drop all secret/credential/cipher framing. Four questions retargeted to the settings gate (existence-only vs. malformed YAML). See §4, Block 4. |
| 13 | Block 5 heading | L83 | Keep | "Dead, ineffective & unconsumed configuration" |
| 14 | Block 5 content | L85–92 | Enrich | Keep core three questions; add checklist items for: dead `merge_dicts` (0 call sites), dead deps (`pandas`, `pydantic-settings`, `python-dateutil`, `urllib3`), doc-code drift on `log_config.yaml` loggers, stale dev `__main__` block. See §4, Block 5. |
| 15 | Block 6 heading | L94 | Modify | "Environment separation & **process parity**" → "Entry-surface parity & config forwarding" |
| 16 | Block 6 content | L96–102 | Rewrite | Drop "credential exposure" and "debug-level logging." Retarget to three real entry surfaces (CLI, library, module). Add: bare `python -m <package>` fails (no `__main__.py`); keep cross-surface `settings_dir` leakage (D-6). See §4, Block 6. |
| 17 | Block 7 heading | L104 | Modify | "Env-overridable production posture" → "Config-driven operational posture" |
| 18 | Block 7 content | L106–113 | Rewrite | Drop "concurrency limit, retry count, credentials file path." Replace with real posture-affecting settings: `data_paths.db_file`, `read_settings.from_csv`, `export_settings.to_csv` (compression, `has_header`). Note: no env-override layer exists. See §4, Block 7. |
| 19 | Severity table | L119–124 | Rewrite | Drop all credential/secret/async examples. CRITICAL → gate-regression guard (empty today); HIGH → malformed-config opaqueness; MEDIUM → dead/unconsumed config + cross-surface leakage; LOW → stale dev block, non-atomic copy, missing `__main__.py`, doc-code drift. See §3.3. |
| 20 | Report Output path | L133 | Modify | `.ai/audit/02-config-secrets/findings.md` → `.ai/audit/02-config/findings.md` |

---

### Application order (per auditor §5 dependency note)

1. **Edits 1–4** (frontmatter, title, Purpose, Scope): foundational —
   removes the async/credential fiction at the source; every block inherits
   framing from the Purpose.
2. **Edit 7–8** (delete Block 2 + gap line): removes the phantom block.
3. **Edits 9–18** (rewrite Blocks 3–7): retarget away from secrets/async.
4. **Edit 19** (Severity taxonomy): re-base examples to the real runtime.
5. **Edit 20** (Report Output path): aligns findings directory with renamed
   phase.

All `src/` paths cited in this document were read from the working tree and
verified as tracked in git. The auditor report (`phase-02-auditor-report.md`)
is the primary source of evidence for every change; no new source-code
inspection is required to apply these edits.
