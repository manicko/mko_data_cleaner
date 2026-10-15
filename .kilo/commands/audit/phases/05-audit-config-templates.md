---
name: 05-config-templates
status: draft
validated: no
executor: auditor
problems-only: true
code_ref: "5c4bfdd"
severity_taxonomy: "05-audit-config-templates.md#severity-taxonomy"
---

# Phase 05 — Config Template Validation

## Purpose

Audits whether the two bundled YAML templates (`app_config.yaml`, `log_config.yaml`)
conform to the Pydantic v2 models that consume them (`DataSettings` at
`models.py:158`; `LoggingSettings` at `models.py:51`), carry types the downstream
Polars reader/writer accepts, have a consistent `extra` policy, and match across
template, model default, and code fallback.

No network, no API, no merge overlay (`merge_dicts` dead — Phase 02). Load path:
`get_app_service()` → `_require_settings()` (existence gate only, `app_service.py:307-322`) →
`DataSettings(**yaml_to_dict(...))` (`app_service.py:47-52`), with
`dictConfig(service.log_config.model_dump())` at `app_service.py:337`.
`yaml_to_dict` (`utils.py:131-149`) returns `{}` on error, masking parse failures as
"field required" downstream.

**Scope:** template↔model↔Polars conformance, field-name drift, `extra`-policy
consistency, default-value divergence, declared-vs-read liveness.
**Out of scope:** template distribution (02); pipeline, export path & crash-safety (04);
fixed-value enum discipline (10); import-time bootstrap & settings gate (01).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. Template-to-model loading & validation fidelity

Load each bundled template against its target model; determine whether a failure
names the offending key or is masked by `extra` policy.

- **Evidence:** both templates pass today (`app_service.py:50,44`); `log_config.yaml`
  dump yields `root.handlers: ['console','rotating_file']` (`log_config.yaml:52-55`).
- **Evidence (induced):** dropping `data_paths` → `ValidationError` naming `data_paths`
  (fail-fast); bogus top-level key silently dropped — `DataSettings` has no `model_config`,
  `extra` defaults to `"ignore"` (`models.py:158-164`).
- **Evidence (gate):** malformed-but-present YAML passes `_require_settings` (existence
  only, `app_service.py:307-322`), fails later as `ConfigError` (`app_service.py:51-52`)
  because `yaml_to_dict` swallows the parse error as `{}` (`utils.py:131-149`).
  Root cause Phase 02; Phase 05 owns the gate-vs-validate surface.

### 2. Field-name drift & extra-policy inconsistency

Map every YAML key → model attribute (incl. nested `to_csv`/`from_csv`); audit the
`extra` setting of every model in the chain.

- `WorkingPaths` = `extra="forbid"` (`models.py:61`);
- `DataFile` / `DataDict` / `Database` / `DictColumnsIndexes` /
  `PolarsReadCSV` / `PolarsWriteCSV` = `extra="allow"` (`models.py:78,143,46,135,94,85`);
- `DataSettings` / `LoggingSettings` = **unset** → Pydantic default `extra="ignore"`
  (`models.py:51,158`).
- **Evidence (absorbed typo):** `to_csv.chunk_sz` under `extra="allow"` silently absorbed →
  consumer falls to its default (`csv_service.py:312`).
- **Evidence (rejected key):** `extra="forbid"` on `WorkingPaths` rejects any key not in
  `import_folder` / `export_folder` / `dict_file` / `db_file` (`models.py:63-67`).

### 3. Reader/Writer parameter type conformance

Whether each template value carries a type the downstream consumer (Polars) accepts.

- **Evidence (gap #2):** `infer_schema_length: False` (`app_config.yaml:36`) vs Polars
  `int | None` (`models.py:102`). `False`→`0`→forces **all columns to `String`**.
  Template comment still says "pandas" (`app_config.yaml:28` — stale).
- **Evidence (bonus):** `compression` Literal `{gzip,bz2,zip,xz,zstd}` (`models.py:90`)
  vs Polars `['uncompressed','gzip','zstd']` — `bz2` → `InvalidOperationError`
  (`csv_service.py:358`); template uses `gzip` (safe).
- **Evidence (gap #4):** `include_bom: True` (`app_config.yaml:43`) is not a declared field
  of `PolarsWriteCSV` (`models.py:84-90`); absorbed by `extra="allow"`, survives `model_dump()`,
  works only because Polars accepts the param — two implicit contracts.

### 4. Template↔model↔code default divergence

No overlay merge exists (`merge_dicts` dead — Phase 02); defaults layer three ways for
`chunk_size`.

- **Evidence:** model default `10000` (`models.py:89`), template `100000`
  (`app_config.yaml:42`), code fallback `params.pop("chunk_size", 10000)` (`csv_service.py:312`).
  Runtime dump shows `100000` (template wins) — the two `10000` literals are a divergent
  second definition; omit `chunk_size` → model default `10000` wins (not code `100000`).

### 5. Extra-field policy & silent absorption

Per-model `extra` behaviour + concrete silent absorption.

- `WorkingPaths` = forbid; six nested models = allow; `DataSettings`/`LoggingSettings`
  = ignore (`models.py:61,51,158`).
- **Evidence (absent field absorbed):** `include_bom` (gap #4, `models.py:84-90`,
  `app_config.yaml:43`) — not a `PolarsWriteCSV` field, carried by `extra="allow"`.
- **Evidence (extra key dropped/substituted):** *extra* top-level key silently dropped by
  `extra="ignore"` (gap #12); typoed `chunk_size` under `extra="allow"` absorbed → code
  fallback `10000` used unseen (`csv_service.py:312`).

### 6. Declared-vs-read conformance

Which validated fields reach a consumer and which are declared-but-never-read.

- **Evidence (read inventory):** `extension` → `CSVWorker._set_files_params` via
  `model_dump().get("extension")` (`csv_service.py:61`); `col_indexes` → `MappingDict`
  (`app_service.py:139`); `from_csv.*` → `pl.read_csv(**self.reader_settings)`
  (`csv_service.py:81,154,206,215`); `to_csv.*` → `df.write_csv(**params)` (`csv_service.py:358`).
- **Evidence (dead declared field):** `fts_separator` (`models.py:147`,
  `app_config.yaml:16`) and `dict_file_settings.extension` have **zero** consumer
  references. Owned by Phase 02; Phase 05 records the template declares what code never reads.
- **Evidence (consumer reads under a different layer):** `chunk_size` via `model_dump().pop`
  (`csv_service.py:312`); `index_column`/`date_column` (`str = None`, gap #1,
  `models.py:80-81`) reach `DBWorker` target-table logic (`db_service.py:76-79`, cross-ref Phase 04).

---

## Severity Taxonomy

Grade by **effect and blast radius**, not mechanism name; rate what is true now. An
empty band is a valid outcome — do not populate it with a hypothetical.

- **CRITICAL** — a shipped template fails validation so the report cannot run while a test asserts it
  should, or a value silently produces wrong output. *Empty today* — both templates validate.
- **HIGH** — a config value silently absorbed/dropped so intent is unmet with no error, or a
  malformed template passes the existence gate, surfacing only an opaque error. **Real
  today (filed as TPL-001):** `yaml_to_dict` returns `{}` on YAML error, masking the cause
  as "field required" `ConfigError` (gap #5, `utils.py:131-149`). *Related, not filed:*
  bogus top-level key silently dropped by `extra="ignore"` (gap #12, TPL-005).
- **MEDIUM** — model accepts a value the downstream consumer rejects or treats differently.
  **Real today:** `infer_schema_length: bool` vs Polars `int | None` → all-`String` (gap #2,
  `models.py:102`, `app_config.yaml:36`); `compression` broader than Polars (bonus, `models.py:90`);
  `include_bom` undeclared & absorbed by `extra="allow"` (gap #4); `extra`-policy inconsistency
  (gap #7, `models.py:61,158,51`); `chunk_size` 3-site divergence (gap #3, `models.py:89`,
  `csv_service.py:312`).
- **LOW** — doc/cosmetic drift, cross-referenced to owning phase: duplicate enum alias
  `csv`≡`default` (gap #11, `models.py:72-73`, Phase 10); stale "pandas" comment
  (`app_config.yaml:28`, Phase 10); `__main__` logger dead config (`log_config.yaml:45`, Phase 02);
  `fts_separator`/`dict_file_settings.extension` declared-but-unread (Phase 02); `DataFile str=None`
  (`models.py:80-81`, Phase 10).

## Report Output

- Findings path: `.ai/audit/05-config-templates/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front matter,
  summary, findings, distribution, cross-finding analysis, roadmap, rollout safety,
  appendices. Front matter carries `code_ref` + `severity_taxonomy` (once, not per finding).
- Finding-ID prefix: `TPL-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only; every finding needs a static proof or
  reproduced error plus the exact consequence. Empty state: `No problems found in this phase.`
- If a shipped test asserts the current behaviour as intended, still file the finding
  and record the test as a remediation blocker.
