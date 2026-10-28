---
phase: "05"
phase_name: "config-templates"
id_prefix: "TPL-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Config Template Validation

## Summary

Ten findings were filed against the bundled `.json5` settings templates and the Pydantic v2 models that consume them. One is HIGH: a malformed-but-present settings file is silently reduced to `{}` by the loader and surfaces as an opaque `pydantic.ValidationError` (not the `ConfigError` the docs promise), masking the real JSON5 syntax error. Four are MEDIUM (Polars type-contract drift on `infer_schema_length`, a `compression` `Literal` broader than Polars accepts, an undeclared `include_bom` field, an inconsistent `extra` policy, and a three-site `chunk_size` divergence). Four are LOW (a redundant enum alias, an inert `__main__` logger, an unread `fts_separator`, and `str = None` annotations). CRITICAL is empty — both shipped templates validate.

## Scope

**Examined:** `core/models.py` (Pydantic models + `extra`/`Literal`/`StrEnum` policy), `core/utils.py::config_to_dict`, `core/app_service.py::AppService.app_config` / `get_app_service` / `_require_settings`, `core/csv_service.py` (Polars reader/writer params, `chunk_size`/`include_bom`), `settings/app_config.json5` + `settings/log_config.json5`, `app.py` (logger absence / entry point), `docs/00-overview/current-behavior.md` and `open-questions.md`.
**Tools:** `grep` (path search), `uv run python` repros against Polars 2.0.0 / Pydantic 2.12.5, `uv run basedpyright src/` (standard), `uv run ruff check src/ tests/` (clean), `uv run pytest` (93 passed).
**Not verified:** the live FTS rule path with real FTS dictionary rows (excluded by phase scope); whether `dict_file_settings.extension` flows in the *merge* path only (confirmed read at `csv_service.py:211`, so not dead).

<!-- GATES: all cited paths verified in `git ls-files` (see write-time check). No `:<digits>` locators in anchor fields. Each "ran" repro has its output in an evidence fence below. -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| TPL-001 | HIGH | `core/utils.py::config_to_dict` | Malformed JSON5 swallowed as `{}` → opaque `ValidationError` masks the parse error |
| TPL-002 | MEDIUM | `core/models.py::PolarsReadCSV.infer_schema_length` | `bool` field makes `false`→`0` (all-`String`) and rejects numeric lengths |
| TPL-003 | MEDIUM | `core/models.py::PolarsWriteCSV.compression` | `compression` `Literal` advertises `bz2/zip/xz` Polars rejects |
| TPL-004 | MEDIUM | `core/models.py::PolarsWriteCSV` | `include_bom` undeclared, forwarded only via `extra="allow"` |
| TPL-005 | MEDIUM | `core/models.py::DataSettings` | Inconsistent `extra` policy silently absorbs typos / drops unknown keys |
| TPL-006 | MEDIUM | `core/csv_service.py::CSVWorker.export_sql_to_csv` | `chunk_size` has three divergent definitions (10000 / 100000 / 10000) |
| TPL-007 | LOW | `core/models.py::DataFileExtension` | `csv` is a redundant alias of `default` |
| TPL-008 | LOW | `settings/log_config.json5::"__main__"` | `__main__` logger is inert (no emitter logs to `__main__`) |
| TPL-009 | LOW | `core/models.py::DataDict.fts_separator` | `fts_separator` declared + templated, never read |
| TPL-010 | LOW | `core/models.py::DataFile` | `index_column`/`date_column` typed `str` but default `None` |

#### TPL-001 — [HIGH] Malformed JSON5 swallowed as `{}` → opaque `ValidationError` masks the parse error

**Anchor:** `src/mko_data_cleaner/core/utils.py::config_to_dict`
**Anchor:** `src/mko_data_cleaner/core/app_service.py::AppService.app_config`
**Disambiguator:** `return {}`
**Checked:** ran
**Why this band:** Phase taxonomy HIGH — "a malformed template passes the existence gate, surfacing only an opaque error." `_require_settings` (existence-only) lets a present-but-broken file through; `config_to_dict` swallows the parse error as `{}`.
**Problem:** `config_to_dict` catches `(FileNotFoundError, ValueError)` and returns `{}` (`utils.py:144-146`). A malformed-but-present `app_config.json5`/`log_config.json5` therefore passes `_require_settings` (`app_service.py:316-323`), yields `{}`, and `DataSettings(**{})` raises a `pydantic.ValidationError` listing missing fields (`data_paths`, …) — the actual JSON5 syntax error is logged at ERROR only and never reaches the exception. The masking is two-layered: (1) the loader returns `{}` on parse failure; (2) `AppService.app_config` wraps construction in `except DataValidationError` (`app_service.py:51-52`), but `DataValidationError` is never raised by Pydantic (Pydantic raises `pydantic.ValidationError`) and `raise DataValidationError` has **0** matches in `src/`/`tests/` — so the `except` is dead and the error is **not** wrapped as `ConfigError`. This contradicts `current-behavior.md:286` ("Pydantic validation error wrapped in `ConfigError`").
**Repro:** (1) write a malformed `app_config.json5` (`{ this is : not valid ,,`); (2) copy a valid `log_config.json5` beside it; (3) `service = get_app_service(settings_dir=tmp)`; (4) access `service.app_config`.
**Impact:** A user with a one-character JSON5 typo is told "data_paths field required" and chases a missing-field error instead of a syntax error — wasted on-call/debug time. The dead `except` also breaks the documented `ConfigError` contract.
**Root cause:** loader swallows parse errors as `{}` (`utils.py:141-146`); the validation `except` catches the wrong exception type (`app_service.py:51-52`).
**Fix:** (a) `config_to_dict` should distinguish *missing* file (→`{}`) from a *parse error* (→ raise/re-raise a `ConfigError` carrying the JSON5 parse location); (b) `AppService.app_config` should `except pydantic.ValidationError` (and the domain `DataValidationError` if any) and wrap as `ConfigError`.
**Verify:** `tests/test_app.py::test_config_to_dict_returns_empty_dict_on_invalid_json5` and `::_on_missing_file` assert the `{}`-return on invalid/missing input — **remediation blockers** that must be updated by the owning phase.
**Related:** gap #12 (bogus top-level key dropped by `extra="ignore"`, verified real but "not filed" per Phase 05 taxonomy — see Fix Order); `DataValidationError` except already filed dead elsewhere (ENT-001/CFG-001).

**Evidence — `configure malformed app_config.json5` then read `service.app_config`** *(supports: masking)*:
```text
<json5 parser: "string:1 Unexpected \"i\" at column 8">  (logged via logger.error in config_to_dict)
RAW ValidationError
message: 6 validation errors for DataSettings
  data_paths    Field required [type=missing, input_value={}, input_type=dict]
  data_file_settings Field required [type=missing, input_value={}, input_type=dict]
  ... (4 more required fields)
caught by except DataValidationError? False
```

#### TPL-002 — [MEDIUM] `PolarsReadCSV.infer_schema_length` is `bool`; `false`→`0` forces all-`String`

**Anchor:** `src/mko_data_cleaner/core/models.py::PolarsReadCSV.infer_schema_length`
**Anchor:** `src/mko_data_cleaner/settings/app_config.json5::"infer_schema_length"`
**Disambiguator:** `infer_schema_length: bool = False`
**Checked:** ran
**Why this band:** Phase taxonomy MEDIUM — "model accepts a value the downstream consumer treats differently." Shipped `false` is accepted by the model but Polars coerces `False`→`0`, disabling inference.
**Problem:** `infer_schema_length` is typed `bool = False` (`models.py:102`), but Polars `read_csv` expects `int | None`. The shipped template value `false` is passed straight through `reader_settings` → `pl.read_csv(**reader_settings)` (`csv_service.py:81/206/215`), where Polars treats `False` as `0` (infer from zero rows) → **every column becomes `String`**, including numeric `YEAR`/`MONTH`. A user also cannot set a real length: `PolarsReadCSV(infer_schema_length=1000)` is rejected by Pydantic (`bool_parsing`). Note: this is load-bearing for the app today because `_is_date_column` (`csv_service.py`) only inspects `pl.String` columns — but that is an accident of the bad default, not a design statement.
**Repro:** read a CSV with an `Int64`-inferable column using `infer_schema_length=False`, `0`, and `50`; construct `PolarsReadCSV(infer_schema_length=1000)`.
**Impact:** Analysts receive text columns where numbers are expected; the `bool` type both silently disables inference (template) and forbids a correct numeric value (user).
**Root cause:** field type chosen to match neither Polars (`int | None`) nor a valid `bool` consumer.
**Fix:** `infer_schema_length: int | NonNegativeInt | None = None` on `PolarsReadCSV`; set the template to `null` (Polars default 100) or a real integer. Template value is part of the fix.
**Verify:** new test `test_infer_schema_length_model_accepts_int`. No shipped test asserts the `bool` type.
**Related:** TPL-006 (default-divergence class), Phase 10.

**Evidence — Polars 2.0.0 dtype inference** *(supports: all-String consequence)*:
```text
infer_schema_length=False -> dtypes: {'a': 'String', 'b': 'String'}
infer_schema_length=0     -> dtypes: {'a': 'String', 'b': 'String'}
infer_schema_length=50    -> dtypes: {'a': 'Int64', 'b': 'String'}
PolarsReadCSV(infer_schema_length=1000) -> ValidationError: Input should be a valid boolean [type=bool_parsing]
runtime dump (shipped template): infer_schema_length=False
```

#### TPL-003 — [MEDIUM] `PolarsWriteCSV.compression` advertises formats Polars rejects

**Anchor:** `src/mko_data_cleaner/core/models.py::PolarsWriteCSV.compression`
**Disambiguator:** `Literal["gzip", "bz2", "zip", "xz", "zstd"]`
**Checked:** ran
**Why this band:** Phase taxonomy MEDIUM — "model accepts a value the downstream consumer rejects." Model validates `compression: "bz2"`; Polars rejects it at export.
**Problem:** `compression` is `Literal["gzip","bz2","zip","xz","zstd"] | None = "gzip"` (`models.py:91`). Polars 2.0 `DataFrame.write_csv` only accepts `gzip`/`zstd`/`uncompressed`; `bz2`/`zip`/`xz` raise `InvalidOperationError: Invalid compression format`. The shipped template uses `gzip` (safe), so this is latent: any user switching to `bz2`/`zip`/`xz` passes model validation, then crashes at `df.write_csv(**params)` (`csv_service.py:364`).
**Repro:** call `pl.DataFrame.write_csv(io.BytesIO(), compression=c)` for each `c`.
**Impact:** a config-validated setting fails at the export stage with a Polars error instead of being rejected at load.
**Root cause:** `Literal` predates/neglects Polars 2.0's `write_csv` compression set.
**Fix:** narrow to `Literal["uncompressed","gzip","zstd"] | None = "gzip"` (Polars 2.0 `write_csv` set); realign `CSVWorker.get_files_suffix` (`csv_service.py:251`) if an alias is dropped.
**Verify:** new test `test_compression_literal_matches_polars_write_csv`. No shipped test uses non-`gzip`.
**Related:** TPL-002 (model-vs-Polars type drift).

**Evidence — Polars 2.0.0 `write_csv` (BytesIO)** *(supports: bz2/zip/xz rejected)*:
```text
write_csv compression gzip -> OK
write_csv compression zstd -> OK
write_csv compression bz2  -> REJECTED InvalidOperationError Invalid compression format: (bz2)
write_csv compression zip  -> REJECTED InvalidOperationError Invalid compression format: (zip)
write_csv compression xz   -> REJECTED InvalidOperationError Invalid compression format: (xz)
```

#### TPL-004 — [MEDIUM] `include_bom` is undeclared on `PolarsWriteCSV`, forwarded only via `extra="allow"`

**Anchor:** `src/mko_data_cleaner/core/models.py::PolarsWriteCSV`
**Anchor:** `src/mko_data_cleaner/settings/app_config.json5::"include_bom"`
**Disambiguator:** `extra="allow"`
**Checked:** ran
**Why this band:** Phase taxonomy MEDIUM — silent absorption via `extra="allow"`; works only through two implicit contracts.
**Problem:** `PolarsWriteCSV` declares `separator, decimal_comma, include_header, chunk_size, compression` (`models.py:85-91`) with `model_config = ConfigDict(extra="allow")`. `include_bom: true` (`app_config.json5:41`) is **not** a declared field; it is absorbed into `__pydantic_extra__`, survives `model_dump()`, and reaches `df.write_csv(**params)` (`csv_service.py:364`). It functions only because (a) `extra="allow"` forwards it, and (b) Polars `write_csv` happens to accept `include_bom` — neither of which is type-checked or documented on the model.
**Repro:** `PolarsWriteCSV(include_bom=True)` then inspect `model_fields` and `model_dump()`.
**Impact:** `include_bom` is untyped/undocumented; a future Polars rename surfaces as an opaque runtime failure with no config-validation hint.
**Root cause:** field routed through `extra="allow"` instead of being declared.
**Fix:** declare `include_bom: bool = False` on `PolarsWriteCSV` (non-breaking).
**Verify:** no shipped test asserts `include_bom`. Add `test_polars_write_csv_declares_include_bom`.
**Related:** TPL-005 (extra-absorption), Phase 10.

**Evidence — Pydantic 2.12.5** *(supports: undeclared, survives dump)*:
```text
"include_bom" in PolarsWriteCSV.model_fields -> False
PolarsWriteCSV(include_bom=True).model_dump()["include_bom"] -> True
runtime dump (shipped template): include_bom=True
df.write_csv(..., include_bom=True) -> OK   (Polars 2.0.0)
```

#### TPL-005 — [MEDIUM] Inconsistent `extra` policy silently absorbs typos / drops unknown keys

**Anchor:** `src/mko_data_cleaner/core/models.py::DataSettings`
**Disambiguator:** `extra="ignore"` (DataSettings has no `model_config`)
**Checked:** ran
**Why this band:** Phase taxonomy MEDIUM — "a config value silently absorbed/dropped so intent is unmet with no error" (the forbid/allow/ignore split). This is the policy root for gap #12.
**Problem:** `extra` is set ad hoc: `WorkingPaths`=forbid (`models.py:62`); `DataFile`/`DataDict`/`Database`/`DictColumnsIndexes`/`PolarsReadCSV`/`PolarsWriteCSV`=allow (`models.py:47,79,86,95,147,155`); `DataSettings`/`LoggingSettings`=unset→Pydantic `ignore` (`models.py:170,52`). Consequences: (a) a typoed top-level key is silently dropped by `extra="ignore"` on `DataSettings`; (b) a typoed nested key (`to_csv.chunk_sz`) is silently absorbed by `extra="allow"` and falls to the code default.
**Repro:** `DataSettings(**{**shipped, "bogus_xyz": 1})` succeeds (key dropped); `PolarsWriteCSV(chunk_sz=100000).model_dump()` carries `chunk_sz` not `chunk_size`, so `csv_service` `params.pop("chunk_size", 10000)` → `10000` (intended `100000` lost).
**Impact:** silent config drift — typoed settings quietly degrade to defaults; diagnosing config mistakes is hard because nothing complains.
**Root cause:** no single `extra` strategy; top-level models rely on Pydantic's permissive default.
**Fix:** adopt one policy project-wide (prefer `extra="forbid"` at the top level and on nested models, keeping the explicit `rechunk` strip at `models.py:104-114`). At minimum set `model_config = ConfigDict(extra="forbid")` on `DataSettings`/`LoggingSettings` to fail fast on typos.
**Verify:** new test `test_data_settings_rejects_unknown_top_level_key`.
**Related:** TPL-001 (masking), gap #12 (verified, not-filed per taxonomy — see Fix Order).

**Evidence — Pydantic 2.12.5** *(supports: bogus drop + typo absorption)*:
```text
DataSettings extra (unset -> ignore): None
DataSettings(**{**shipped,'bogus_xyz':1}) -> OK (bogus_xyz dropped, no error)
PolarsWriteCSV extra: allow
dump chunk_size: 10000 | dump chunk_sz: 100000
params.pop("chunk_size", 10000) => 10000  (intended 100000 lost)
```

#### TPL-006 — [MEDIUM] `chunk_size` has three divergent definitions

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::CSVWorker.export_sql_to_csv`
**Disambiguator:** `params.pop("chunk_size", 10000)`
**Checked:** ran (runtime dump) + static
**Why this band:** Phase taxonomy MEDIUM — default-value divergence across template/model/code.
**Problem:** `chunk_size` is defined three ways: model default `10000` (`models.py:90`); template `100000` (`app_config.json5:40`); code fallback `params.pop("chunk_size", 10000)` (`csv_service.py:315`). Runtime dump yields `100000` (template wins). If the template line is omitted, the model default `10000` wins — **not** a `100000` (that literal exists only in the template). The code `pop(...,10000)` is a redundant second `10000` that can never differ from the model default in the current flow. A fourth site, `CSVWorker.CHUNK_SIZE = 10000` (`csv_service.py:28`), is the *read-scan* batch size (a separate concern).
**Repro:** load shipped template → `export_settings.to_csv.model_dump()["chunk_size"]`.
**Impact:** two `10000` literals and one `100000` can silently drift; changing the intended export batch size requires edits in three places and a reader must know which wins.
**Root cause:** no single source of truth for the export batch size.
**Fix:** make the model default the single source (choose `10000` or `100000` deliberately, matching the template intent) and remove the `params.pop("chunk_size", 10000)` fallback (the key is always present from the model); align `CSVWorker.CHUNK_SIZE` naming.
**Verify:** `test_csv_service.py` sets `chunk_size` explicitly (e.g. `1`), so no test asserts a default; add a test pinning model default vs template.
**Related:** TPL-002 (default-divergence class).

**Evidence — shipped template runtime dump** *(supports: template wins = 100000)*:
```text
template chunk_size (runtime dump): 100000   (model field default would be 10000 if omitted)
template infer_schema_length (runtime dump): False
template include_bom (runtime dump): True
```

#### TPL-007 — [LOW] `DataFileExtension.csv` is a redundant alias of `default`

**Anchor:** `src/mko_data_cleaner/core/models.py::DataFileExtension`
**Disambiguator:** `csv = "csv"`
**Checked:** ran
**Why this band:** Phase taxonomy LOW — cosmetic enum discipline; cross-ref Phase 10.
**Problem:** `DataFileExtension` declares `default = "csv"` and `csv = "csv"` (`models.py:72-74`). A repeated enum value makes `csv` an alias of `default`; iterating members yields only `default`/`gz`, and `DataFileExtension.csv == DataFileExtension.default`. Two names for one value is a maintenance trap.
**Repro:** `[(m.name, m.value) for m in DataFileExtension]`; compare `.csv == .default`.
**Impact:** none behavioral; confusing for readers and brittle if the value later diverges.
**Root cause:** legacy alias retained from the predecessor project.
**Fix:** drop the `csv` alias (keep `default`/`gz`) or make `csv` canonical and alias `default` to it. Phase 10 (enum discipline) owns the design call.
**Verify:** no shipped test references `DataFileExtension.csv` (only `.gz` at `test_app.py:224`).
**Related:** TPL-010 (DataFile type discipline), Phase 10.

**Evidence — Pydantic 2.12.5 / enum** *(supports: csv aliases default)*:
```text
aliases: [('default', 'csv'), ('gz', 'gz')]
csv == default? True   |   csv.value: csv
```

#### TPL-008 — [LOW] Inert `__main__` logger in `log_config.json5`

**Anchor:** `src/mko_data_cleaner/settings/log_config.json5::"__main__"`
**Disambiguator:** `"__main__"`
**Checked:** ran + static
**Why this band:** Phase taxonomy LOW — dead config; cross-ref Phase 02.
**Problem:** `log_config.json5:45` defines `loggers.__main__` ("handlers: rotating_file"). No module ever emits to a `__main__` logger: every module uses `logging.getLogger(__name__)` (`utils.py`, `app_service.py`, `csv_service.py`, …) → `mko_data_cleaner.*`, and `app.py` has **no** `logging` import or logger at all. The CLI runs via the Typer entry point `mko-data-cleaner = "mko_data_cleaner.app:app"` (`pyproject.toml:45`), not `python app.py`, so even the `if __name__ == "__main__": app()` guard (`app.py:153`) creates no `__main__` logger. The entry is never matched → inert config copied to every user's settings on `init`.
**Repro:** `grep -rn "__main__" src/mko_data_cleaner/core` (only `if __name__` blocks, no `getLogger("__main__")`); confirm `pyproject.toml` entry point.
**Impact:** template pollution — every user copies an unused logger; it obscures whether a real `__main__` handler is intended. `open-questions.md:110` already tracks stale loggers here (the `tests`/`telebot` entries since removed; `__main__` is the current residue).
**Root cause:** leftover from the predecessor project's `__main__` bootstrap; not pruned when Phase 01 reformed import-time bootstrap.
**Fix:** remove the `loggers.__main__` block from `log_config.json5`. Phase 02 (bootstrap) owns confirmation no `__main__` logger is needed.
**Verify:** no shipped test asserts the `__main__` logger.
**Related:** TPL-007 (template hygiene), Phase 02.

**Evidence — grep + pyproject** *(supports: no emitter -> inert)*:
```text
settings/log_config.json5:45: "__main__": { // if __name__ == "__main__"
src/.../app.py:153: if __name__ == "__main__":   (no getLogger(__main__) anywhere in src/)
pyproject.toml:45: mko-data-cleaner = "mko_data_cleaner.app:app"
```

#### TPL-009 — [LOW] `DataDict.fts_separator` declared + templated, never read

**Anchor:** `src/mko_data_cleaner/core/models.py::DataDict.fts_separator`
**Disambiguator:** `fts_separator: str = "|"`
**Checked:** ran
**Why this band:** Phase taxonomy LOW — declared-but-unread; cross-ref Phase 02.
**Problem:** `fts_separator` is a `DataDict` field (`models.py:159`) with a template value `app_config.json5:16`, but `grep "fts_separator" src/` returns only those two occurrences — no consumer reads it. *Auditor correction:* the Phase 05 draft also claims `dict_file_settings.extension` is unread, but it IS read at `csv_service.py:211` (`self.dict_settings.get("extension", …)` in `get_merged_dictionary`); only `fts_separator` is dead.
**Repro:** `grep -rn "fts_separator" src/mko_data_cleaner`.
**Impact:** config declares a knob a user can set, but the code never honors it — a silent contract violation if a user changes the separator.
**Root cause:** field never wired to the FTS query builder (`dict_service.py`).
**Fix:** wire `fts_separator` into FTS query construction in `dict_service.py`, or remove the field + template key. Phase 02 (FTS/merging) owns.
**Verify:** no shipped test references `fts_separator`.
**Related:** Phase 02.

**Evidence — grep** *(supports: zero consumer references)*:
```text
settings/app_config.json5:16: "fts_separator": "|"
core/models.py:159: fts_separator: str = "|"
(no other matches in src/)
```

#### TPL-010 — [LOW] `DataFile.index_column`/`date_column` typed `str` but default `None`

**Anchor:** `src/mko_data_cleaner/core/models.py::DataFile`
**Disambiguator:** `: str = None`
**Checked:** ran + static (basedpyright)
**Why this band:** Phase taxonomy LOW — type-discipline drift; cross-ref Phase 10.
**Problem:** `index_column: str = None` and `date_column: str = None` (`models.py:81-82`). The annotation says `str` but the default is `None`; Pydantic silently makes the field nullable. `basedpyright` (standard) flags `models.py:81:25` and `:82:24`: "Type `None` is not assignable to declared type `str`". At runtime `DataFile(extension='csv')` yields `index_column=None, date_column=None` — a value that cannot satisfy the declared `str` type.
**Repro:** construct `DataFile(extension='csv')`; run `basedpyright`.
**Impact:** the schema lies about nullability; DB callers (`db_service.py:76-79` via `app_service.py:143-149`) must None-guard fields typed `str`.
**Root cause:** missing `| None` in the annotation.
**Fix:** `index_column: str | None = None` / `date_column: str | None = None`. Phase 10 (type discipline) owns.
**Verify:** `test_app.py:222` asserts `index_column == "AdId"` (a str); annotating `str | None` keeps that green.
**Related:** TPL-007 (DataFile type discipline), Phase 10.

**Evidence — Pydantic 2.12.5 + basedpyright** *(supports: None allowed despite str)*:
```text
DataFile(extension='csv').model_dump() -> {'extension':'csv','index_column':None,'date_column':None}
basedpyright: models.py:81:25 - error: Type "None" is not assignable to declared type "str"
              models.py:82:24 - error: Type "None" is not assignable to declared type "str"
```

## Fix Order

- **Dependencies:** TPL-001 must precede TPL-005 — tightening `extra` (TPL-005) before fixing the loader mask (TPL-001) would turn previously-silent bogus keys into `ValidationError`s that still surface opaquely if `config_to_dict` keeps returning `{}`. TPL-002/003/004/006 (model-field fixes) land together in `models.py` (+ `app_config.json5` for TPL-002/TPL-006 values).
- **Merge candidates:** TPL-002, TPL-003, TPL-004, TPL-006 — all "model fields drift from the consumer contract"; one coordinated pass on `models.py`/template.
- **Conflicting evidence:** none. (Auditor corrections recorded above: the Phase 05 draft's "pandas" comment, `models.py:102`-as-`int|None`, and `dict_file_settings.extension`-is-dead claims are stale for `c0fd0e1`.)
- **Cross-phase conflicts:** gap #12 (top-level key dropped by `extra="ignore"`) is verified real but explicitly "not filed" by the Phase 05 taxonomy; it is the symptom side of TPL-005's policy root cause and TPL-001's masking class — do **not** open a separate `TPL-` for it. `DataValidationError` dead-`except` already filed (ENT-001/CFG-001); related to TPL-001. Phase 10 owns TPL-007/004/010 type+enum discipline; Phase 02 owns TPL-008/009/TPL-001-root-cause.
- **Rollout risk:** TPL-005 (`extra="forbid"` on `DataSettings`) may reject existing user configs that rely on absorbed keys — gate behind a release note + optional migrate. TPL-001's loader change is blocked by `test_app.py::test_config_to_dict_returns_empty_dict_on_invalid_json5` and `::_on_missing_file`; those tests assert the `{}`-return and must be rewritten (distinguish missing→`{}` from parse-error→raise) before the fix lands.

## Appendices

### Appendix A — Phase 05 evidence corrections (c0fd0e1 vs phase `code_ref` 5c4bfdd)

The Phase 05 task prose was authored at `5c4bfdd`; the audited tree is `c0fd0e1`. The following draft claims are stale and were corrected by runtime/static verification above rather than filed verbatim:
- "YAML templates (`app_config.yaml`/`log_config.yaml`)" / `yaml_to_dict` → actual artifacts are `app_config.json5`/`log_config.json5` parsed by `config_to_dict` (json5), per `paths.py` (`app_config`/`log_config` properties) and `app_service.py:44,48`.
- "stale 'pandas' comment (`app_config.yaml:28`)" → no `pandas` token exists in `src/` or `docs/`; the `read_settings` comment now reads "polars CSV reader" (`app_config.json5:27`). Not filed.
- "`models.py:102` = `int | None`" → line 102 is actually `infer_schema_length: bool = False` (the defect, not the ideal type).
- "`dict_file_settings.extension` zero consumer references" → read at `csv_service.py:211`; only `fts_separator` (TPL-009) is unread.

### Appendix B — Malformed-config reproduction (TPL-001)

```text
$ uv run python -c "<malformed app_config.json5 + valid log_config.json5 via get_app_service; access svc.app_config>"
<string>:1 Unexpected "i" at column 8        # config_to_dict logs the parse error, returns {}
RAW ValidationError
message: 6 validation errors for DataSettings
  data_paths             Field required [type=missing, input_value={}, input_type=dict]
  data_file_settings     Field required [type=missing, ...]
  dict_file_settings     Field required ...
  database_settings      Field required ...
  read_settings          Field required ...
  export_settings        Field required ...
caught by except DataValidationError? False     # app_service.py:51 except is dead
is pydantic ValidationError? True
```

### Appendix C — Polars 2.0.0 type-conformance repro (TPL-002, TPL-003, TPL-004)

```text
infer_schema_length=False -> dtypes: {'a': 'String', 'b': 'String'}   (all String)
infer_schema_length=50    -> dtypes: {'a': 'Int64', 'b': 'String'}
PolarsReadCSV(infer_schema_length=1000) -> ValidationError: bool_parsing
write_csv compression gzip -> OK ; zstd -> OK ; bz2/zip/xz -> InvalidOperationError
write_csv include_bom=True -> OK
"include_bom" in PolarsWriteCSV.model_fields -> False
```

### Appendix D — Extra-policy + chunk_size absorption (TPL-005, TPL-006)

```text
DataSettings extra (unset -> ignore): None
DataSettings(**{**shipped,'bogus_xyz':1}) -> OK (bogus_xyz silently dropped)
PolarsWriteCSV extra: allow ; WorkingPaths extra: forbid
PolarsWriteCSV(chunk_sz=100000).model_dump() -> {'chunk_size':10000,'chunk_sz':100000}
params.pop("chunk_size", 10000) => 10000   (intended 100000 lost)
shipped template dump: chunk_size=100000, infer_schema_length=False, include_bom=True
```

### Appendix E — Baseline tooling (GATES)

```text
$ git rev-parse --short HEAD  ->  c0fd0e1
$ uv run ruff check src/ tests/  ->  All checks passed!
$ uv run basedpyright src/       ->  39 errors, 0 warnings  (incl. models.py:81:25, :82:24 str=None)
$ uv run pytest                   ->  93 passed in 3.76s
```
