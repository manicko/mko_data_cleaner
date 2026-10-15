---
phase: "05"
phase_name: "Config Template Validation"
report_status: "draft"
code_ref: "5c4bfdd"
phase_file: ".kilo/commands/audit/phases/05-audit-config-templates.md"
---

# Audit Report — Phase 05 (rework) — Config Template Validation

**What this report does.** The Phase 05 phase file (`05-audit-config-templates.md`)
describes an *async, Mediascope-API* config model (`ReportSettings`/`PeriodModel`/
`LastTimeModel`/`DataDefaults`/`Data`, a `settings/reports/` + `settings/defaults/`
tree, a defaults-merge overlay, `date_filter`/`last_time` period resolution, `kitID`).
**None of that exists.** The shipped system is a synchronous, single-shot, local-only
tool whose entire config surface is two package YAML templates validated by Pydantic v2.
This report audits the Phase 05 *doc* against the actual architecture, assesses the
13 verification gaps, and proposes a reworked block set, severity taxonomy, report
output, and cross-phase citations. No source files were modified.

## Reality: the current config system (evidence)

Two bundled YAML templates (package data, copied to the user settings dir by `init`):

| Template | Target model | Location |
|---|---|---|
| `app_config.yaml` | `DataSettings` (root) | `src/mko_data_cleaner/settings/app_config.yaml` |
| `log_config.yaml` | `LoggingSettings` | `src/mko_data_cleaner/settings/log_config.yaml` |

- `DataSettings` root model (`core/models.py:158-164`) wraps six nested models:
  `WorkingPaths`, `DataFile`, `DataDict`, `Database`, `ReadCSV`→`PolarsReadCSV`,
  `WriteCSV`→`PolarsWriteCSV`. There is **no** `settings/reports/`, **no**
  `settings/defaults/`, **no** `ReportSettings`/`DataDefaults`/`Data`/`Period`/`LastTime`
  model, **no** merge overlay (`merge_dicts` at `core/utils.py:152-173` is dead — 0 call
  sites, owned by Phase 02), **no** `date_filter`/`last_time`/`kitId`/`kitID`.
- Load path: `get_app_service` (`:293-338`) → `_require_settings` (existence gate,
  `:307-322`) → `AppService.app_config` cached_property
  (`app_service.py:47-52`) = `DataSettings(**utils.yaml_to_dict(self.app_paths.app_config))`,
  with `DataValidationError` wrapped as `ConfigError`. `dictConfig(service.log_config.model_dump())`
  at `app_service.py:337`.

---

## Summary table — 6 current blocks → Fit / Outdated / Replacement

| # | Current block title | Fit | Outdated (async/API concepts absent from code) | Replacement (new block, renumbered) |
|---|---|---|---|---|
| 1 | Template-to-Pydantic model conformance | **Partial** | `settings/reports/` report templates vs `ReportSettings`→`PeriodModel`→`LastTimeModel`; `settings/defaults/` vs `DataDefaults`; "merged `Data` validated before reaching the API call." No report/default dirs; no Period/LastTime/Data; no API call. | **New 1** — Template-to-model loading & validation fidelity (`app_config.yaml`→`DataSettings`, `log_config.yaml`→`LoggingSettings`). |
| 2 | Field-name drift between templates and models | **Partial** | "period subtype field," `date_filter`/`last_time` examples; `extra="forbid"`/`"allow"` framing built on the phantasm models. `extra` policy is real, but the model inventory is wrong. | **New 2** — Field-name drift & extra-policy inconsistency (correct model inventory: WorkingPaths forbid vs siblings allow vs DataSettings/LoggingSettings default-ignore). |
| 3 | Period / last_time model conformance | **None** | `PeriodModel`→`LastTimeModel`, `date_filter`/`last_time` mutual exclusivity, `time_unit`/`unit_count`/`include_current`. The date column is auto-detected from data (`csv_service.py:124-157`), not configured by a period model. | **New 3** (repurposed) — Reader/Writer parameter type conformance: template value types vs the Polars signatures the code actually passes. |
| 4 | Template-field conformance within the merge | **None** | "defaults-to-Data merge," "merged `Data` for one report," "missing default file." No merge exists; no `Data` model; `merge_dicts` is dead (Phase 02). | **New 4** — Template↔model↔code default divergence (no overlay; but a 3-layer default stack: model default / template value / code fallback). |
| 5 | Extra-field policy and silent absorption | **Partial** | Lists `ReportSettings`/`PeriodModel`/`LastTimeModel`/`DataDefaults`/`Data` — none exist. The *question* ("does a typoed/unknown key get silently absorbed?") is valid. | **New 5** — Extra-field policy & silent absorption (refocused onto the real model set; one absorbed typo, one dropped unknown key). |
| 6 | Declared spec conformance against what the code reads | **Partial** | "merged `Data` model" premise. Concept ("does the code read what the template declares?") is valid. | **New 6** — Declared-vs-read conformance: which declared fields reach a consumer and which are dead (cross-ref Phase 02 for the broader dead-config sweep). |

> Note on overlap: Blocks 2 & 5 and Blocks 4 & 6 each touch the same surface
> (extra-policy / defaults). In the rework they are split by *mechanism*: Block 2 =
> **name drift**, Block 5 = **`extra` policy behaviour**, Block 4 = **default-value
> layering**, Block 6 = **consumer-read liveness**. None re-opens Phase 02's
> `merge_dicts` / dead-dependency / `yaml_to_dict`-fallback root causes.

---

## Proposed new block set (renumbered 1–6)

Each block is independent and evidence-classed.

### New 1 — Template-to-Pydantic model loading & validation fidelity
Load each bundled template and validate against its target model; determine whether a
failure names the offending key or is masked by `extra` policy.
- **Evidence:** `app_config.yaml`→`DataSettings` and `log_config.yaml`→`LoggingSettings`
  both pass today (verified: `DataSettings(**yaml.safe_load(app_config.yaml))` succeeds;
  `LoggingSettings(**yaml.safe_load(log_config.yaml))` succeeds, dump yields
  `loggers:['__main__']`, `root.handlers:['console','rotating_file']`).
- **Evidence (induced failure):** dropping `data_paths` yields a `ValidationError`
  naming `data_paths` (*good*); but adding a bogus top-level key is silently dropped
  (see New 2 — `DataSettings` has no `model_config`, defaults to `extra="ignore"`).
- **Evidence (gate-vs-validate):** a malformed-but-present YAML passes
  `_require_settings` (existence only, `app_service.py:307-322`) and only fails later
  as `ConfigError` (New 1 is where the "passes lens" check lives; root cause is
  `yaml_to_dict` returning `{}`, Phase 02).

### New 2 — Field-name drift & extra-policy inconsistency
Map every YAML key→model attribute (incl. nested `from_csv`/`to_csv`) and audit the
`extra` setting of every model in the chain.
- **Evidence (inventory):** `WorkingPaths` = `extra="forbid"` (`models.py:61`);
  `DataFile`/`DataDict`/`Database`/`DictColumnsIndexes`/`PolarsReadCSV`/`PolarsWriteCSV`
  = `extra="allow"` (`models.py:78,85,94,135,143`); `DataSettings`/`LoggingSettings` =
  **unset** → Pydantic default `extra="ignore"` (`models.py:51,158`, verified: a bogus
  top-level key is dropped from `model_dump()`). **Inconsistent surface.**
- **Evidence (absorbed typo):** a mistyped nested key under `extra="allow"`, e.g.
  `to_csv.chunk_sz: 100000`, is silently absorbed → the consumer falls to its own
  default. See New 5 / gap #3 for the `chunk_size` silent-fallback chain.
- **Evidence (rejected valid key):** `extra="forbid"` on `WorkingPaths` rejects any key
  not in `import_folder/export_folder/dict_file/db_file` — confirm a harmless extra key
  under `data_paths` raises `ValidationError`.

### New 3 — Reader/Writer parameter type conformance (repurposed from "Period/last_time")
Whether each template value carries a type the downstream consumer (Polars) accepts.
This is the true analog of the old "period model validates the template's period
specification" — template value → consumer signature, not PeriodModel→LastTimeModel.
- **Evidence:** `infer_schema_length: False` (`app_config.yaml:36`) vs Polars
  `int | None` (`models.py:102`) — verified `False` coerces to `0`, forcing **all
  columns to `String`** (`pl.read_csv(infer_schema_length=False)` → `Schema all String`).
- **Evidence:** `compression` Literal `{gzip,bz2,zip,xz,zstd}` (`models.py:90`) vs
  Polars `Literal['uncompressed','gzip','zstd']` — verified `compression='bz2'` raises
  `InvalidOperationError` at `df.write_csv`; YAML uses `gzip` (passes).
- **Evidence:** `include_bom: True` (`app_config.yaml:43`) is **not** a declared field of
  `PolarsWriteCSV` (`models.py:84-90`) — absorbed by `extra="allow"` and carried to
  `write_csv` only because `model_dump()` includes extras (verified) and Polars accepts
  the param (verified). Works now via two implicit contracts.

### New 4 — Template↔model↔code default divergence (repurposed from "within the merge")
No overlay merge exists (`merge_dicts` is dead — Phase 02), but effective defaults are
layered three ways for at least `chunk_size`.
- **Evidence:** model default `chunk_size: NonNegativeInt = 10000` (`models.py:89`);
  template `chunk_size: 100000` (`app_config.yaml:42`); code fallback
  `params.pop("chunk_size", 10000)` (`csv_service.py:312`). Verified at runtime the
  dumped value is `100000` (template wins). The two `10000` literals (model default +
  code fallback) are a second, divergent definition of the same constant.
- **Evidence (missing key):** if the template omits `chunk_size`, the model default
  `10000` (not the code `100000`) would win — three different possible values depending
  on which tier supplies the key.

### New 5 — Extra-field policy & silent absorption (refocused from "extra-field policy")
Per-model `extra` behaviour + concrete silent absorption.
- **Evidence (per-model):** `WorkingPaths` forbid; six models allow; `DataSettings`/
  `LoggingSettings` ignore.
- **Evidence (one unknown key silently absorbed):** `include_bom` (undeclared on
  `PolarsWriteCSV`, gap #4); a bogus top-level key on `DataSettings` (gap #12).
- **Evidence (one consumed key silently dropped):** a typoed `chunk_size` under
  `extra="allow"` is absorbed and the code fallback `10000` is used unseen
  (`csv_service.py:312`) — silent operator-intent loss, no error.

### New 6 — Declared-vs-read conformance (refocused from "declared spec")
Which validated fields reach a consumer, vs. declared-but-never-read fields.
- **Evidence (read inventory):** `data_file_settings.extension` →
  `CSVWorker._set_files_params` via `model_dump().get("extension")` (`csv_service.py:61`);
  `dict_file_settings.col_indexes` → `app_service.py:139` → `MappingDict`;
  `read_settings.from_csv.*` → `pl.read_csv(**self.reader_settings)` (`csv_service.py:81,154,206,215`);
  `export_settings.to_csv.*` → `df.write_csv(**params)` (`csv_service.py:356-361`).
- **Evidence (dead declared field):** `fts_separator` (`models.py:147`) and
  `dict_file_settings.extension` appear in the template+model but have **zero** consumer
  references (grep: `fts_separator` only at `app_config.yaml:16` + `models.py:147`;
  `dict_file_settings.extension` not read; the dict is loaded at `dict_path`
  `app_service.py:75-76` = `merged_dictionary.csv`). Owned as dead-config by Phase 02;
  Phase 05 records the template declares what the code never reads.
- **Evidence (consumer reads under a different layer):** `chunk_size` is read via
  `model_dump().pop`, not the model attribute; `index_column`/`date_column` (`str = None`,
  gap #1) reach `DBWorker` (`app_service.py` run_report), so a `None` default lands in DB
  target-table logic (`db_service.py:76-79`). Cross-ref Phase 04.

---

## Assessment of the 13 key gaps (verified)

| # | Claim in task | Reality (code) | Verified? | Severity | Phase / owner |
|---|---|---|---|---|---|
| 1 | `DataFile.index_column: str = None`, `date_column: str = None` — annotation `str`, default `None` | `models.py:80-81`. Pydantic v2 accepts `None` default despite `str` annotation (runtime: `index_column default: None`). Type-checkers flag `None` assigned to `str`; a `None` also flows into `DBWorker` target-table logic (`db_service.py:76-79`). | **Yes** (empirical) | LOW | Phase 10 (type-width, Block 4) |
| 2 | `PolarsReadCSV.infer_schema_length: bool = False` — Polars wants `int \| None` | `models.py:102`; `app_config.yaml:36`. Polars 1.38.1 sig is `infer_schema_length: int \| None = 100`. `False`→`0`→forces **all columns to `String`** (verified: `df.schema` all String). Comment in template still says "pandas" (stale). | **Yes** (Polars sig + runtime) | MEDIUM | Phase 05 (New 3) |
| 3 | Model default `chunk_size=10000` vs template `100000` | `models.py:89`; `app_config.yaml:42`. Template wins (dumped `100000`). Model default is dead when template is present. | **Yes** | MEDIUM | Phase 05 (New 4) |
| 4 | `include_bom: True` in YAML; `PolarsWriteCSV` has no `include_bom` field; absorbed by `extra="allow"`; does Polars accept it? | `include_bom` **not** in `PolarsWriteCSV.model_fields` (fields: separator/decimal_comma/include_header/chunk_size/compression). Absorbed by `extra="allow"` (`models.py:85`); survives `model_dump()` (verified: `'include_bom' in dump == True`); Polars `write_csv` **does** accept `include_bom: bool` (docs + works). So export succeeds via two implicit contracts. | **Yes** (model_dump + Polars docs) | MEDIUM | Phase 05 (New 2/5) |
| 5 | `yaml_to_dict()` returns `{}` on `FileNotFoundError`/`YAMLError` — silently swallows config errors | `utils.py:131-149` (returns `{}` on both). Shipped tests **assert** this mask: `test_app.py:212-221` (`test_yaml_to_dict_returns_empty_dict_on_missing_file`, `..._on_invalid_yaml`). Downstream `{}`→`DataSettings(**{})`→`ValidationError`→`ConfigError` (`app_service.py:47-52`) — user sees "field required", not "your YAML is invalid." | **Yes** (code + test) | HIGH | Phase 02 (root cause) / Phase 05 gate angle |
| 6 | `chunk_size` `NonNegativeInt` OK; YAML `100000` fine; `export_sql_to_csv` uses `params.pop("chunk_size", 10000)` — YAML value used, fallback differs | `csv_service.py:312`. YAML `100000` is used (dumped value). The code fallback `10000` (3rd definition) is a latent silent-substitute if the key is typo'd under `extra="allow"`. | **Yes** | — | sub-gaps #3, #5 (New 5) |
| 7 | `WorkingPaths` `extra="forbid"` vs siblings `extra="allow"` — inconsistent | `models.py:61` (forbid), `:78/85/94/135/143` (allow), `:51/158` unset→ignore. Verified `extra="ignore"` drops bogus top-level key; `extra="allow"` absorbs nested extras. Inconsistent policy surface. | **Yes** | MEDIUM | Phase 05 (New 2/5); Phase 10 Block 3 |
| 8 | `log_config.yaml` `filename: "log.log"` relative — resolved to `user_dir`? | `log_config.yaml:37`. `prepare_log_paths` (`app_service.py:82-88`) rewrites each handler `filename` via `self.resolver.resolve(...)` → absolute under `user_dir` before `dictConfig` (`app_service.py:337`). Relative is intentional and resolved; not a defect. | **Yes** (code path) | Informational / not a defect | — |
| 9 | `_copy_templates` uses global `APP_PATHS.app_settings_dir` regardless of `settings_dir` — correct? | `init_service.py:12`. The global is the **source** (package templates = `APP_DIR/settings`). The **destination** is param-derived: `init_project` picks `APP_PATHS.user_settings_dir` or `AppPaths(..., user_dir=settings_dir).user_settings_dir` (`init_service.py:44-50`). Source-always-packaged, dest-varies is correct. | **Yes** (verified) | Informational / not a defect | Phase 01 (entry parity) |
| 10 | `log_config.yaml` `__main__` logger; app uses `getLogger("app_service")` / `getLogger(__name__)` — never `__main__` | `log_config.yaml:45`. Logger names in code: `app_service` (`app_service.py:32`) + `mko_data_cleaner.core.{db_service,csv_service,dict_service,utils}` via `__name__` (4 modules). No logger named `__main__`. The `__main__` block is dead config; real loggers are covered only by `root` (`log_config.yaml:52-55`). Logs still work via root propagation. | **Yes** (grep) | LOW | Phase 02 (dead-config; doc-code drift already noted Phase 02 §1) |
| 11 | `DataFileExtension.Self` has `default="csv"`, `csv="csv"` — duplicate value | No `Self` member exists. But `models.py:72-73` define `default = "csv"` **and** `csv = "csv"` — verified `DataFileExtension.csv is DataFileExtension.default` (`True`, alias). Duplicate-value enum is real; the `Self` reference is stale. | **Yes** (enum alias) | LOW | Phase 10 (fixed-value enum, Block 1) |
| 12 | `DataSettings` has no `model_config` → `extra="ignore"` → unknown top-level keys silently absorbed | `models.py:158-164` has no `model_config`. Verified: bogus top-level key dropped from `model_dump()`. A typoed *required* top-level key (e.g. `data_path`) → `field required` error (fail-fast); an *extra* typoed key → silent. | **Yes** (empirical) | MEDIUM | Phase 05 (New 2/5) |
| 13 | `extension: 'gz'` for both data_file & dict_file; `DataFileExtension` only has `default`/`csv`/`gz` — is `gz` valid? | `DataFileExtension.gz = "gz"` (`models.py:74`). `gz` is valid; template loads (`DataSettings(**cfg)` succeeds). `csv` is an alias of `default` (gap #11). | **Yes** | Not a defect | — |

**Bonus (in-scope for New 3/Phase 05, not in the 13):** `PolarsWriteCSV.compression` is
typed `Literal["gzip","bz2","zip","xz","zstd"] | None` (`models.py:90`) but Polars
`write_csv` accepts only `['uncompressed','gzip','zstd']` (verified: `bz2` →
`InvalidOperationError` at runtime). Template uses `gzip` (safe). Latent operator footgun.

---

## Severity Taxonomy (rebased — synchronous, config-specific)

The current taxonomy (Phase 05 §Severity) is built on the async fiction
(`kitId`, `date_filter`, `Data` model, API call, defaults-merge dropping subtype
overrides). Rebased to the real, synchronous, file→SQLite pipeline:

- **CRITICAL** — a shipped template/example fails validation so the report cannot run,
  while a test asserts it should work; or a template value silently produces materially
  wrong output with **no** error and no guard. **Empty today** (both templates validate;
  no template silently wrong-outputs). *Regression guard only — must stay empty.*
- **HIGH** — a config value is silently absorbed/dropped so the operator's intent is
  unmet with no error; or a malformed-but-present template passes the existence gate and
  surfaces only as an opaque downstream error. **Real today:**
  `yaml_to_dict` returns `{}` on syntax error, masking the cause as
  "field required" `ConfigError` (gap #5; root cause Phase 02, but the gate-vs-validate
  surface is Phase 05); a typoed nested key under `extra="allow"` (e.g. `chunk_sz`)
  absorbed with the code fallback silently substituting a different value (gap #3/#6).
- **MEDIUM** — latent type/policy divergence where the model accepts a value the
  downstream consumer rejects or treats differently than its annotation states. **Real
  today:** `infer_schema_length: bool` vs Polars `int|None`→all-String (gap #2);
  `compression` Literal broader than Polars (bonus); `include_bom` undeclared extra
  relying on the implicit `model_dump()`-includes-extras contract (gap #4);
  `extra`-policy inconsistency forbid/allow/ignore (gap #7); `chunk_size` 3-site
  default divergence (gap #3); `DataSettings` default `extra="ignore"` swallowing
  unknown top-level keys (gap #12).
- **LOW** — doc/cosmetic/no-runtime-impact drift, cross-referenced to the owning phase:
  duplicate enum alias `csv`≡`default` (gap #11, Phase 10); stale "pandas" comment
  (`app_config.yaml:28`, Phase 10 Block 7); `__main__` logger dead config
  (`log_config.yaml:45`, Phase 02); `fts_separator`/`dict_file_settings.extension`
  declared-but-unread (Phase 02 dead-config); `DataFile str=None` annotation
  (`models.py:80-81`, Phase 10).

---

## Report Output (assessment)

The Phase 05 §Report Output is already aligned with the repo convention and needs
**no change** — it is the parts *other* than the block/taxonomy content that are
correct:

| Item | Current (Phase 05 §Report Output) | Verdict |
|---|---|---|
| Findings path | `.ai/audit/05-config-templates/findings.md` | **Correct** — matches `.ai/audit/NN-name/findings.md` (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`). |
| ID prefix | `TPL-` | **Correct** — config-template prefix; unique in the repo (no existing `TPL-` collisions). |
| problems-only | `true` (front matter line 6 + §line 140) | **Correct.** |
| per-pass lines | ≤100 | **Correct.** |
| Empty state | `No problems found in this phase.` | **Correct** (verbatim). |
| Shipped-test-asserts-current-behaviour | recorded as remediation blocker | **Correct** — applies to gap #5 (`test_app.py:212-221`). |

Only the **front matter** should be reconciled with the findings template
(`.ai/audit/templates/audit-findings.md`): it currently omits the
`severity_taxonomy` / `code_ref` front-matter fields the template defines. Recommend
adding `code_ref: "5c4bfdd"` and `severity_taxonomy` once the phase file is reworked.

---

## Cross-phase citation cleanup (lines 27-29)

Current ownership line:
> Owned elsewhere: template distribution and credential sourcing (02); the data
> pipeline and export correctness (04); fixed-value enum discipline (10); import-time
> bootstrap (01).

Phase 02 has been re-based to **Configuration Management** (no credentials; `02-audit-config.md`).
Phase 04 is the (rebased) **Data Pipeline** (synchronous). Phase 10 is **Code Quality**
(fixed values / enum discipline). Phase 01 is **Entry Architecture** (import-time + gate).

Proposed replacement:
> Owned elsewhere: **template distribution (02)**; the data pipeline, export path &
> crash-safety (04); **fixed-value enum discipline (10)**; **import-time bootstrap and
> the settings gate (01)**.

- (02) **REWORD.** "credential sourcing" has no target (no credentials anywhere — Phase 02
  §1 confirms). "template distribution" is valid: `init`→`init_project`→`_copy_templates`
  (`init_service.py:30-53`) is owned by Phase 02 (Block 1). Drop the credential phrase.
- (04) **KEEP.** Correct — pipeline & export are Phase 04. Tighten "export correctness" →
  "export path & crash-safety" (Phase 04 covers `csv_service.py` non-atomic writes,
  `db_service.py` DB teardown).
- (10) **KEEP.** "fixed-value enum discipline" maps to Phase 10 Block 1
  ("Fixed values: one named home per value"). The `DataFileExtension` duplicate
  `default`/`csv` alias (gap #11) is exactly this phase's scope — file there, don't
  double-file here.
- (01) **KEEP.** "import-time bootstrap" → Phase 01 owns the lazy factory and the
  settings gate (`get_app_service` / `_require_settings`, `app_service.py:293-338`).
  Retighten to "import-time bootstrap and the settings gate (01)".

**Reworked Purpose (recommended doc edit):** replace the async/Mediascope/API/merge
sentences (lines 13-22) with:
> Audits whether the two bundled YAML templates (`app_config.yaml`,
> `log_config.yaml`) conform to the Pydantic v2 models that consume them
> (`DataSettings` at `core/models.py:158`; `LoggingSettings` at `core/models.py:51`),
> whether template values carry types the downstream Polars reader/writer accepts,
> whether the `extra` policy of each model is consistent, and whether template/model
> defaults diverge across the template, the model default, and the code fallback. There
> is **no** report/default template tree, **no** `ReportSettings`/`PeriodModel`/
> `LastTimeModel`/`Data` model, **no** defaults-merge overlay (`merge_dicts` is dead —
> see Phase 02), and **no** `kitId`/`date_filter`/`last_time` period resolution. Owned
> here: template↔model↔Polars conformance, field-name drift, `extra`-policy
> consistency, default-value divergence. Owned elsewhere: template distribution (02);
> the data pipeline & export (04); fixed-value enum discipline (10); import-time
> bootstrap & the settings gate (01).

---

## Verification notes (evidence pedigree)

- `git show HEAD:pyproject.toml` → `polars>=1.38.1`, `pydantic>=2.12.5`; runtime
  resolved to polars **1.38.1**.
- Runtime checks performed against the working tree (code_ref `5c4bfdd`), **no source
  files modified**:
  - `DataSettings(**yaml.safe_load(app_config.yaml))` → succeeds; `dump['chunk_size']=100000`,
    `'include_bom' in dump==True`; `WorkingPaths` defaults; `DataFile`
    `index_column/date_column` default `None`.
  - `LoggingSettings(**yaml.safe_load(log_config.yaml))` → succeeds;
    `loggers=['__main__']`, `root.handlers=['console','rotating_file']`.
  - `DataFileExtension.csv is DataFileExtension.default` → `True`.
  - `DataFile(extension='gz')` (no index/date) → `index_column=None` (str annotation
    accepts None).
  - `pl.read_csv(infer_schema_length=False)` → schema all `String`;
    `pl.DataFrame.write_csv(compression='bz2')` → `InvalidOperationError`.
- GATES satisfied: every cited `path::Symbol` resolves in `git ls-files`; disambiguator
  tokens verified inside each primary anchor.

---

## Concrete doc edits to `.kilo/commands/audit/phases/05-audit-config-templates.md`

1. **[Purpose §13-22]** Replace async/Mediascope/API/merge prose with the Reworked Purpose
   above (removes `ReportSettings`/`PeriodModel`/`LastTimeModel`/`DataDefaults`/`Data`,
   `settings/reports/`, `settings/defaults/`, `date_filter`/`last_time`/`kitId`).
2. **[Ownership line §27-29]** Apply the proposed replacement (drop credential sourcing;
   tighten (04)/(01)).
3. **Delete Block 3's premise** (§64-74) — no `PeriodModel`/`LastTimeModel` exists;
   re-theme as New 3 (Reader/Writer param type conformance) per the table above.
4. **Delete Block 4's premise** (§76-84) — no defaults-merge; re-theme as New 4
   (default divergence) per the table above.
5. **Refocus Blocks 1,2,5,6** to the real model set (`DataSettings`/`LoggingSettings`,
   `WorkingPaths`/`DataFile`/`DataDict`/`Database`/`PolarsReadCSV`/`PolarsWriteCSV`/
   `DictColumnsIndexes`) per the proposed new block set.
6. **[Severity Taxonomy §110-130]** Replace every example (all async-API grounded) with
   the config-specific bands in §Severity Taxonomy above; keep CRITICAL as an empty
   regression guard.
7. **[Severity examples to drop]** all of `kitId`, `date_filter`, "defaults-merge that
   drops a subtype-specific override", "template passes lenient parsing but fails strict
   validation" (no API/lenient parser here).
8. **[Front matter]** add `code_ref` and `severity_taxonomy` fields to match the
   findings template; keep `problems-only: true`.
9. **[Report Output §132-144]** no change required (path/prefix/problems-only already
   correct); only the front-matter enrichment in item 8.

No source files modified. Gaps #5 (yaml_to_dict `{}`), #10 (`__main__` logger), #11
(duplicate enum), and the dead `merge_dicts`/`fts_separator` roots live in Phase 02/10;
this phase owns the template↔model↔Polars conformance angles (New 1/3/4/5/6).
