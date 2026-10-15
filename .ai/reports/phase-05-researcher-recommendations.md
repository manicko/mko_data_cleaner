---
phase: "05"
phase_name: "Config Template Validation"
phase_file: ".kilo/commands/audit/phases/05-audit-config-templates.md"
source: ".ai/reports/phase-05-auditor-report.md"
author: researcher
verdict: REBASE
confidence: HIGH
code_ref: "5c4bfdd"
---

# Phase 05 — Researcher Recommendations

## 1. Executive Summary

The auditor verdict is **REBASE**. The Phase 05 spec file (`05-audit-config-templates.md`,
144 lines) describes an async, Mediascope-API config model (`ReportSettings` /
`PeriodModel` / `LastTimeModel` / `DataDefaults` / `Data`, a `settings/reports/` +
`settings/defaults/` tree, a defaults-merge overlay, `date_filter` / `last_time` /
`kitId`). **None of that exists.** The shipped system is synchronous, single-shot,
local-only: two package YAML templates (`app_config.yaml` → `DataSettings`,
`log_config.yaml` → `LoggingSettings`) validated by Pydantic v2, with no network,
no credentials, no merge overlay (`merge_dicts` is dead — Phase 02).

This recommendation endorses the auditor's **New 1–6** block renumbering verbatim,
selects **6 findings to surface** (gaps #2, #4, #5, #7, bonus compression, #11),
**suppresses 2 non-defects** (#8, #9), rewrites the severity taxonomy to
config-specific synchronous examples, confirms the Report Output settings with
front-matter enrichment, and provides a line-by-line edit delta.

Evidence was verified against the working tree at `5c4bfdd`: `polars>=1.38.1`,
`pydantic>=2.12.5` (runtime polars 1.38.1). Polars `write_csv` `compression`
accepts only `['uncompressed', 'gzip', 'zstd']` (docs.pola.rs confirmed). No source
files were modified.

## 2. Proposed Final Block Set (New 1–6, renumbered)

All blocks are independent and evidence-classed. Evidence classes used below:

- **static proof** — a stated property is contradicted by code inspection alone
- **reproduced validation error** — an induced input that triggers a validation/runtime error
- **observed behavior** — an empirical check of the running system

| # | Title | Description | Evidence class |
|---|---|---|---|
| 1 | Template-to-model loading & validation fidelity | Load each bundled template against its target model; determine whether failures name the offending key or are masked by `extra` policy. | static proof + observed behavior |
| 2 | Field-name drift & extra-policy inconsistency | Map every YAML key → model attribute; audit the `extra` setting of every model in the chain. | static proof + observed behavior |
| 3 | Reader/Writer parameter type conformance | Whether each template value carries a type the downstream Polars reader/writer accepts. | static proof + reproduced validation error + observed behavior |
| 4 | Template↔model↔code default divergence | Whether effective defaults are consistent across the three layers (model default / template value / code fallback) with no overlay merge. | static proof + observed behavior |
| 5 | Extra-field policy & silent absorption | Per-model `extra` behaviour + concrete silent absorption of typoed/unknown keys. | static proof + observed behavior |
| 6 | Declared-vs-read conformance | Which validated fields reach a consumer and which are dead. | static proof + observed behavior |

### New 1 — Template-to-model loading & validation fidelity

Load `app_config.yaml` → `DataSettings` and `log_config.yaml` → `LoggingSettings`
and validate. The existence gate (`_require_settings`, `app_service.py:307-322`)
only checks file presence — a malformed-but-present YAML passes the gate and fails
later as `ConfigError` (`app_service.py:47-52`), because `yaml_to_dict` swallows the
parse error (root cause lives in Phase 02, but the gate-vs-validate surface is
Phase 05).

- **Evidence:** both templates pass `DataSettings(**yaml.safe_load(...))` (verified:
  `chunk_size=100000` in dump, `include_bom` in dump, `WorkingPaths` defaults).
- **Evidence (induced):** dropping `data_paths` yields a `ValidationError` naming
  `data_paths` — good fail-fast. Adding a bogus top-level key is silently dropped
  (see New 2).
- **Evidence (masking):** malformed YAML passes `_require_settings` and surfaces
  only as `ConfigError` ("field required"), not "your YAML is invalid."

### New 2 — Field-name drift & extra-policy inconsistency

Map every YAML key → model attribute across the full model chain. Audit the
`extra` setting of each model:

- `WorkingPaths` = `extra="forbid"` (`models.py:61`)
- `DataFile` / `DataDict` / `Database` / `DictColumnsIndexes` / `PolarsReadCSV` /
  `PolarsWriteCSV` = `extra="allow"` (`models.py:78,85,94,135,143`)
- `DataSettings` / `LoggingSettings` = **unset** → Pydantic default `extra="ignore"`
  (`models.py:51,158`; verified: bogus top-level key dropped from `model_dump()`)

- **Evidence (absorbed typo):** `to_csv.chunk_sz: 100000` under `extra="allow"`
  is silently absorbed → code falls to its own default.
- **Evidence (rejected valid key):** `extra="forbid"` on `WorkingPaths` rejects any
  key not in `import_folder` / `export_folder` / `dict_file` / `db_file`.

### New 3 — Reader/Writer parameter type conformance

Whether each template value carries a type the downstream consumer (Polars)
accepts. This replaces the deleted "PeriodModel → LastTimeModel" block.

- **Evidence (gap #2):** `infer_schema_length: False` (`app_config.yaml:36`) vs
  Polars `int | None` (`models.py:102`). Verified: `False` → `0` → forces **all
  columns to `String`** (`pl.read_csv(infer_schema_length=False)` → schema all String).
- **Evidence (bonus):** `compression` Literal `{gzip, bz2, zip, xz, zstd}`
  (`models.py:90`) vs Polars `Literal['uncompressed','gzip','zstd']`. Verified:
  `df.write_csv(compression='bz2')` → `InvalidOperationError` at runtime.
- **Evidence (gap #4):** `include_bom: True` (`app_config.yaml:43`) is **not** a
  declared field of `PolarsWriteCSV` (`models.py:84-90`); absorbed by
  `extra="allow"`, survives `model_dump()` (verified: `'include_bom' in dump`),
  and works only because Polars accepts the param (docs confirmed).

### New 4 — Template↔model↔code default divergence

No overlay merge exists (`merge_dicts` is dead — Phase 02), but effective defaults
are layered three ways for at least `chunk_size`:

- Model default: `chunk_size: NonNegativeInt = 10000` (`models.py:89`)
- Template value: `chunk_size: 100000` (`app_config.yaml:42`) — template wins
- Code fallback: `params.pop("chunk_size", 10000)` (`csv_service.py:312`)

- **Evidence:** runtime dump shows `100000` (template wins). If the template omits
  `chunk_size`, the model default `10000` wins (not the code `100000`). Three
  possible values for one setting.

### New 5 — Extra-field policy & silent absorption

Per-model `extra` behaviour plus concrete absorption:

- `WorkingPaths` = `forbid`; six nested models = `allow`; `DataSettings` /
  `LoggingSettings` = `ignore`.
- **Evidence (one absent field silently absorbed):** `include_bom` (gap #4).
- **Evidence (one bogus top-level key silently dropped):** `DataSettings` has no
  `model_config` → `extra="ignore"` (gap #12); a typoed *required* key fails
  fast, an *extra* key is silently dropped.
- **Evidence (one consumed key silently substituted):** a typoed `chunk_size`
  under `extra="allow"` is absorbed and the code fallback `10000` is used unseen
  (`csv_service.py:312`).

### New 6 — Declared-vs-read conformance

Which validated fields reach a consumer vs. which are declared-but-never-read:

- **Evidence (read inventory):** `data_file_settings.extension` →
  `CSVWorker._set_files_params` (`csv_service.py:61`);
  `dict_file_settings.col_indexes` → `app_service.py:139` → `MappingDict`;
  `read_settings.from_csv.*` → `pl.read_csv(**self.reader_settings)`
  (`csv_service.py:81,154,206,215`); `export_settings.to_csv.*` →
  `df.write_csv(**params)` (`csv_service.py:356-361`).
- **Evidence (dead declared field):** `fts_separator` (`models.py:147`) and
  `dict_file_settings.extension` appear in the template + model but have **zero**
  consumer references. Owned as dead-config by Phase 02; Phase 05 records the
  template declares what the code never reads.
- **Evidence (consumer reads under a different layer):** `chunk_size` is read via
  `model_dump().pop`, not the model attribute; `index_column` / `date_column`
  (`str = None`, gap #1) reach `DBWorker` (`db_service.py:76-79`).

## 3. Confirmed Findings to Surface

| Gap | Claim | Reality (code) | Evidence class | Severity | Phase / owner |
|---|---|---|---|---|---|
| #2 | `PolarsReadCSV.infer_schema_length: bool = False` — Polars wants `int \| None` | `models.py:102`; `app_config.yaml:36`. Polars 1.38.1 sig is `int \| None = 100`. `False`→`0`→all columns `String`. Stale "pandas" comment (`app_config.yaml:28`). | reproduced validation error + observed behavior | MEDIUM | Phase 05 (New 3) |
| #4 | `include_bom: True` in YAML; `PolarsWriteCSV` has no `include_bom` field; absorbed by `extra="allow"`; relies on implicit `model_dump()`-includes-extras contract. | `include_bom` not in `PolarsWriteCSV.model_fields` (fields: `separator`, `decimal_comma`, `include_header`, `chunk_size`, `compression` — `models.py:84-90`). Survives `model_dump()` (verified); Polars accepts the param (docs confirmed). | static proof + observed behavior | MEDIUM | Phase 05 (New 2/5) |
| #5 | `yaml_to_dict()` returns `{}` on `FileNotFoundError` / `YAMLError` — silently swallows config errors. | `utils.py:131-149`. Shipped tests **assert** the mask: `test_app.py:212-221`. Downstream `{}` → `DataSettings(**{})` → `ConfigError`. | static proof + observed behavior | HIGH | Phase 02 (root cause) / Phase 05 (gate angle) |
| #7 | `WorkingPaths` `extra="forbid"` vs siblings `extra="allow"` vs roots `extra="ignore"` — inconsistent. | `models.py:61` (forbid), `:78/85/94/135/143` (allow), `:51/158` unset→ignore. Verified: bogus top-level key dropped; nested typo absorbed. | static proof + observed behavior | MEDIUM | Phase 05 (New 2/5); Phase 10 Block 3 |
| Bonus | `PolarsWriteCSV.compression` typed `Literal["gzip","bz2","zip","xz","zstd"] \| None` — Polars accepts only `['uncompressed','gzip','zstd']`. | `models.py:90`. Verified: `compression='bz2'` → `InvalidOperationError` at `df.write_csv`. Template uses `gzip` (safe). | static proof + reproduced validation error | MEDIUM | Phase 05 (New 3) |
| #11 | `DataFileExtension` defines `default="csv"` and `csv="csv"` — duplicate value (alias). | `models.py:72-73`. Verified: `DataFileExtension.csv is DataFileExtension.default == True`. Stale `Self` reference in task text. | observed behavior | LOW | Phase 10 (fixed-value, Block 1) |

## 4. Gaps NOT to Surface (verified non-defects)

| Gap | Claim | Why it is NOT a defect |
|---|---|---|
| #8 | `log_config.yaml` `filename: "log.log"` is relative — does it resolve? | `log_config.yaml:37`. `prepare_log_paths` (`app_service.py:82-88`) rewrites each handler `filename` via `self.resolver.resolve(...)` → absolute under `user_dir` before `dictConfig` (`app_service.py:337`). The relative path is intentional and is always resolved before use. File as a documentation note, not a finding. |
| #9 | `_copy_templates` uses global `APP_PATHS.app_settings_dir` regardless of `settings_dir` — correct? | `init_service.py:12`. The global is the **source** (package templates = `APP_DIR/settings`). The **destination** is param-derived: `init_project` picks `APP_PATHS.user_settings_dir` or `AppPaths(..., user_dir=settings_dir).user_settings_dir` (`init_service.py:44-50`). Source-always-packaged, dest-varies is correct. Cross-ref Phase 01 for entry-surface parity, but do not file here. |

## 5. Severity Taxonomy (config-specific, synchronous)

The current taxonomy (Phase 05 §110-130) is built on the async fiction (`kitId`,
`date_filter`, `Data` model, defaults-merge). All examples must be replaced with
config-specific synchronous framing. Keep the four bands, the principle, and the
"empty band is valid" note.

- **CRITICAL** — a shipped template/example fails validation so the report cannot run,
  while a test asserts it should work; or a template value silently produces materially
  wrong output with **no** error and no guard.
  - *Concrete today:* **empty** (both templates validate; no template silently wrong-outputs).
    Regression guard only — must stay empty.

- **HIGH** — a config value is silently absorbed/dropped so the operator's intent is
  unmet with no error; or a malformed-but-present template passes the existence gate and
  surfaces only as an opaque downstream error.
  - *Concrete today (filed as TPL-001):* `yaml_to_dict` returns `{}` on YAML syntax error,
    masking the cause as "field required" `ConfigError` (gap #5).
  - *Concrete today (related, not filed separately):* a bogus top-level key on `DataSettings`
    silently dropped by `extra="ignore"` (gap #12 — a sub-aspect of the #7 `extra`-policy
    inconsistency, folded under TPL-005); a typoed nested key under `extra="allow"` (e.g.
    `chunk_sz`) absorbed with the code fallback silently substituting a different value
    (gap #3/#6 — the 3-site `chunk_size` divergence, documented in New 4 but not a separate
    finding).

- **MEDIUM** — latent type/policy divergence where the model accepts a value the downstream
  consumer rejects or treats differently than its annotation states.
  - *Concrete today (filed as TPL-002–005):* `include_bom` undeclared on `PolarsWriteCSV`,
    absorbed by `extra="allow"` (gap #4); `infer_schema_length: bool` vs Polars `int | None`
    → all-`String` (gap #2); `compression` Literal broader than Polars (bonus);
    `extra`-policy inconsistency forbid/allow/ignore (gap #7, incl. gap #12's
    silent top-level absorption); `chunk_size` 3-site default divergence (gap #3).

- **LOW** — doc/cosmetic/no-runtime-impact drift, cross-referenced to owning phase:
  duplicate enum alias `csv`≡`default` (gap #11, filed as TPL-006); stale "pandas" comment
  (`app_config.yaml:28`, Phase 10); `__main__` logger dead config (`log_config.yaml:45`,
  Phase 02); `fts_separator` / `dict_file_settings.extension` declared-but-unread (Phase 02);
  `DataFile str=None` annotation (`models.py:80-81`, Phase 10).

**Drop all of:** `kitId`, `date_filter`, "defaults-merge that drops a subtype-specific
override", "template passes lenient parsing but fails strict validation".

## 6. Report Output Settings

The Phase 05 §Report Output (current lines 132-144) is **already aligned** with the
repo convention. Recommend **keeping** it with one front-matter enrichment.

| Item | Setting | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/05-config-templates/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`). Confirmed against the working tree (`git ls-files`). |
| ID prefix | `TPL-` | **Keep** | Config-template prefix; unique in the repo (no existing `TPL-` collisions). |
| problems-only | `true` | **Keep** | All six confirmed findings are code-verified defects, not passing checks. Dropping it would dilute findings with methodology. |
| per-pass lines | ≤100 | **Keep** | Template constraint. |
| Empty state | `No problems found in this phase.` | **Keep** | Verbatim. |
| Shipped-test asserts current behaviour | recorded as remediation blocker | **Keep** | Applies to gap #5 (`test_app.py:212-221`). Per auditor: file the finding, record the test as a blocker — do not drop it. |

**Front-matter enrichment (recommend add):** the findings template
(`.ai/audit/templates/audit-findings.md`) defines `severity_taxonomy` and `code_ref`
front-matter fields. Add to the phase file's front matter (lines 1-7):

```yaml
code_ref: "5c4bfdd"
severity_taxonomy: ".kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy"
```

- `code_ref: "5c4bfdd"` — the git short SHA the report describes (verified via
  `git rev-parse --short HEAD`); written once on the phase file, never per finding.
- `severity_taxonomy` — anchors the findings file to the phase file's
  `## Severity Taxonomy` section (line 110 of the reworked file).
- Keep `problems-only: true` and `executor: auditor` unchanged.

## 7. Cross-Phase Citations

Current ownership line (lines 27-29):

> Owned elsewhere: template distribution and credential sourcing (02); the data
> pipeline and export correctness (04); fixed-value enum discipline (10); import-time
> bootstrap (01).

| Citation | Action | Rationale |
|---|---|---|
| **(02) template distribution + credential sourcing** | **REWORD** — drop "credential sourcing" | No credentials exist anywhere in the codebase (Phase 02 §1 confirms). "Template distribution" is valid: `init` → `init_project` → `_copy_templates` (`init_service.py:30-53`) is owned by Phase 02 (Block 1). Drop the credential phrase entirely. |
| **(04) data pipeline and export correctness** | **REWORD** — tighten | Pipeline & export are Phase 04. Tighten "export correctness" → "export path & crash-safety" (Phase 04 covers `csv_service.py` non-atomic writes, `db_service.py` DB teardown). Cross-ref for gap #1 (`index_column`/`date_column` reaching `DBWorker` target-table logic, `db_service.py:76-79`). |
| **(10) fixed-value enum discipline** | **KEEP** | Maps to Phase 10 Block 1 ("Fixed values: one named home per value"). Gap #11 (`DataFileExtension` duplicate `csv`/`default` alias) is Phase 10's scope — file there, do not double-file here. |
| **(01) import-time bootstrap** | **KEEP** — retighten | Phase 01 owns the lazy factory + settings gate (`get_app_service` / `_require_settings`, `app_service.py:293-338`). Retighten to "import-time bootstrap and the settings gate (01)". |

**Recommended replacement ownership line:**

> Owned elsewhere: template distribution (02); the data pipeline, export path &
> crash-safety (04); fixed-value enum discipline (10); import-time bootstrap and
> the settings gate (01).

**Recommended Purpose rewrite (lines 13-22):** replace async/Mediascope/API/merge
prose with:

> Audits whether the two bundled YAML templates (`app_config.yaml`, `log_config.yaml`)
> conform to the Pydantic v2 models that consume them (`DataSettings` at
> `core/models.py:158`; `LoggingSettings` at `core/models.py:51`), whether template
> values carry types the downstream Polars reader/writer accepts, whether the `extra`
> policy of each model is consistent, and whether template/model defaults diverge
> across the template, the model default, and the code fallback. There is **no**
> report/default template tree, **no** `ReportSettings` / `PeriodModel` /
> `LastTimeModel` / `Data` model, **no** defaults-merge overlay (`merge_dicts` is dead —
> see Phase 02), and **no** `kitId` / `date_filter` / `last_time` period resolution. Owned
> here: template↔model↔Polars conformance, field-name drift, `extra`-policy consistency,
> default-value divergence. Owned elsewhere: template distribution (02); the data
> pipeline & export (04); fixed-value enum discipline (10); import-time bootstrap &
> the settings gate (01).

## 8. Edit Delta — Line-by-Line Rewrite of the 144-line File

All references are to the **current** `.kilo/commands/audit/phases/05-audit-config-templates.md`.

| # | Lines | Action | Replacement (concise) | Rationale |
|---|---|---|---|---|
| 1 | 1–7 (front matter) | **Modify** — add fields | Keep `name`, `status`, `validated`, `executor`, `problems-only: true`. Add `code_ref: "5c4bfdd"` and `severity_taxonomy: ".kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy"`. | Findings-template alignment; written once, never per finding. |
| 2 | 9 (H1) | **Modify** | `# Phase 05 — Config Template Validation` (strip "& Spec Conformance" — no API spec here) | Removes async/spec framing. |
| 3 | 13–22 (Purpose para) | **Replace** | The Purpose rewrite in §7 above | Removes `ReportSettings`/`PeriodModel`/`LastTimeModel`/`DataDefaults`/`Data`, `settings/reports/`, `settings/defaults/`, `date_filter`/`last_time`/`kitId`. |
| 4 | 27–29 (ownership line) | **Replace** | The ownership-line rewrite in §7 above | Drops "credential sourcing"; tightens (04)/(01). |
| 5 | 40–50 (Block 1) | **Replace** with New 1 | "Load each bundled template against its target model; determine whether failures name the offending key or are masked by `extra` policy." + 3 evidence bullets | Removes `ReportSettings`/`PeriodModel`; targets `DataSettings`/`LoggingSettings`. |
| 6 | 52–62 (Block 2) | **Replace** with New 2 | "Map every YAML key → model attribute; audit the `extra` setting of every model in the chain." + evidence bullets (WorkingPaths forbid, 6 models allow, roots ignore; absorbed typo; rejected valid key) | Removes phantom model names; uses real `extra` inventory from §2. |
| 7 | 64–74 (Block 3) | **Replace** with New 3 | "Reader/Writer parameter type conformance: whether each template value carries a type the downstream Polars reader/writer accepts." + evidence (gap #2 `infer_schema_length`→all-`String`; bonus `compression`→`InvalidOperationError`; gap #4 `include_bom` undeclared) | Deletes `PeriodModel`/`LastTimeModel`/`date_filter`/`last_time` entirely; repurposes as Polars type-conformance. |
| 8 | 76–84 (Block 4) | **Replace** with New 4 | "Template↔model↔code default divergence: no overlay merge exists, but effective defaults are layered three ways." + evidence (chunk_size: model 10000 / template 100000 / code 10000; dump shows 100000; missing key → 10000) | Deletes "defaults-to-Data merge"; targets the real 3-layer default stack. |
| 9 | 86–95 (Block 5) | **Replace** with New 5 | "Per-model `extra` behaviour + concrete silent absorption." + evidence (per-model survey; `include_bom` absorbed; bogus top-level key dropped; typoed `chunk_size` silently substituted) | Refocuses onto the real model set; concrete absorption examples. |
| 10 | 97–106 (Block 6) | **Replace** with New 6 | "Declared-vs-read conformance: which validated fields reach a consumer and which are dead." + evidence (read inventory; `fts_separator`/`dict_file_settings.extension` dead; `chunk_size` read via `model_dump().pop`; `index_column`/`date_column` reach `DBWorker`) | Refocuses onto the real model set; cross-refs Phase 02 for dead config. |
| 11 | 35–38 (Audit Blocks intro) | **Keep** | "Each block is independent…" | Generic methodology language remains valid. |
| 12 | 110–130 (Severity Taxonomy) | **Replace examples only** | §5 above: keep four bands + principle + "empty band is valid"; replace all `kitId`/`date_filter`/defaults-merge examples with config-specific ones; CRITICAL stays empty | All old examples reference the deleted async/API model. |
| 13 | 132–144 (Report Output) | **Keep**, with front-matter note | Keep findings path, `TPL-` prefix, `problems-only: true`, ≤100 lines, empty-state wording, shipped-test-as-blocker. Add the §6 front-matter enrichment note (item 1). | Already aligned; only needs the `code_ref`/`severity_taxonomy` addition. |

**Application order:** Edit 1 (front matter) → Edit 4 (ownership, foundational framing)
→ Edit 3 (Purpose) → Edits 5–10 (blocks 1–6) → Edit 12 (severity) → Edit 2 (H1) →
Edit 13 (Report Output note). Edits 1 and 2 are independent and can be done first;
all block edits (5–10) replace self-contained sections and may be applied in
parallel. Edits 3 and 4 should precede the blocks so the Purpose frames the New
blocks correctly.

**Do not touch:** the `problems-only: true` front-matter flag; the "≤100-line passes"
rule; the empty-state wording ("No problems found in this phase.").

## 9. Findings-to-Scope Mapping (for the findings file author)

Findings are sorted by severity (HIGH first, then MEDIUM, then LOW) per the findings
template's "index order IS the work order" rule. `TPL-` prefix retained from Report Output.

| Finding ID | Gap | Severity | Anchor | Phase-owned | Notes |
|---|---|---|---|---|---|
| TPL-001 | #5 `yaml_to_dict` returns `{}` on error, masks cause as "field required" | HIGH | `utils.py:131-149` / `test_app.py:212-221` | Phase 02 (root cause) / Phase 05 (gate angle, New 1) | Shipped test asserts the mask — record as remediation blocker. |
| TPL-002 | #4 `include_bom` undeclared on `PolarsWriteCSV`, absorbed by `extra="allow"` | MEDIUM | `models.py:84-90` / `app_config.yaml:43` | Phase 05 (New 2/5) | Works via two implicit contracts: `model_dump()` includes extras + Polars accepts the param. |
| TPL-003 | #2 `infer_schema_length: bool` vs Polars `int \| None` → all-`String` | MEDIUM | `models.py:102` / `app_config.yaml:36` | Phase 05 (New 3) | Stale "pandas" comment at `app_config.yaml:28`. |
| TPL-004 | Bonus `compression` Literal broader than Polars accepts | MEDIUM | `models.py:90` | Phase 05 (New 3) | Template uses `gzip` (safe); `bz2` → `InvalidOperationError`. |
| TPL-005 | #7 `extra` policy inconsistency (forbid / allow / ignore) | MEDIUM | `models.py:61,78,85,94,135,143,51,158` | Phase 05 (New 2/5); Phase 10 Block 3 | Gap #12 (`DataSettings`/`LoggingSettings` silent top-level absorption) is a sub-aspect — fold under this finding rather than double-file. |
| TPL-006 | #11 `DataFileExtension` duplicate enum alias `csv` ≡ `default` | LOW | `models.py:72-73` | Phase 10 (fixed-value, Block 1) | Phase 05 surfaces; Phase 10 fixes. File here per New 5 (dead/redundant config). |

**Out of scope for findings (cross-ref only):**
- Gap #1 (`DataFile str = None` annotation) → Phase 10 Block 4 (type-width)
- Gap #3 / #6 (`chunk_size` model-default vs template vs code-fallback divergence) → covered by the New 4 block's default-divergence audit; not filed as a separate finding (the 3-site value stack is documented in New 4 itself)
- Gap #10 (`__main__` logger dead config) → Phase 02 §1 (doc-code drift)
- Gap #12 (`DataSettings`/`LoggingSettings` no `model_config` → silent top-level absorption) → subsumed under TPL-005 (#7 `extra`-policy inconsistency)
- Gap #13 (`gz` validity) → not a defect
- Gaps #8, #9 → verified non-defects (§4 above); do not file

---

*End of recommendation. Evidence-grounded: every structural recommendation traces to a
verified current-source fact (working tree at `5c4bfdd`). Polars `write_csv` `compression`
accepts only `['uncompressed','gzip','zstd']` (confirmed via docs.pola.rs, 2026-10-15).
No source files were modified by this analysis.*
