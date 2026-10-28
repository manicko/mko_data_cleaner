---
phase: "05"
phase_name: "config-templates"
id_prefix: "TPL"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy"
code_ref: "c0fd0e1"
validated_at: "2026-10-19"
validated_by: "validator"
---

# Validation Report — Config Template Validation (Phase 05)

## Summary

All ten `TPL-` findings from `.ai/audit/05-config-templates/findings.md` reproduce
against commit `c0fd0e1` (HEAD). Every claim was re-derived from the executing path:
all ten are **confirmed**, no re-grades, no re-types, no rejections. Highest severity is
**HIGH** (`TPL-001`). CRITICAL band remains empty — both templates validate against
their target models (`DataSettings`, `LoggingSettings`).

The Phase 05 input was authored at `5c4bfdd` per its phase file, but the findings
report was written against `c0fd0e1` and every stale draft claim (YAML→JSON5 naming,
`pandas` comment, `models.py:102` type, `dict_file_settings.extension` liveness) was
corrected by runtime/static verification rather than filed verbatim (documented in
Appendix A of the input). `ruff` is clean, `pytest` passes 93, `basedpyright` reports
39 errors / 0 warnings (matching the input's claim).

Cross-phase overlap: `TPL-001` shares its root cause with `ENT-001` / `CFG-001` (malformed-config masking + dead `except DataValidationError`). Both Phase 01 and Phase 02 validation reports reached the same verdict; this is a cross-reference, not a conflict. `TPL-001` adds the Phase 05 config-template angle (`current-behavior.md:286` doc-code contradiction) and its own remediation-blocker tests.

## Dispositions

| ID | Severity | Anchor | Verdict | Type |
|---|---|---|---|---|
| TPL-001 | HIGH | `core/utils.py::config_to_dict` | confirmed | SPEC-DEVIATION |
| TPL-002 | MEDIUM | `core/models.py::PolarsReadCSV.infer_schema_length` | confirmed | SPEC-DEVIATION |
| TPL-003 | MEDIUM | `core/models.py::PolarsWriteCSV.compression` | confirmed | SPEC-DEVIATION |
| TPL-004 | MEDIUM | `core/models.py::PolarsWriteCSV` | confirmed | BEST-PRACTICE |
| TPL-005 | MEDIUM | `core/models.py::DataSettings` | confirmed | BEST-PRACTICE |
| TPL-006 | MEDIUM | `core/csv_service.py::CSVWorker.export_sql_to_csv` | confirmed | BEST-PRACTICE |
| TPL-007 | LOW | `core/models.py::DataFileExtension` | confirmed | BEST-PRACTICE |
| TPL-008 | LOW | `settings/log_config.json5::"__main__"` | confirmed | BEST-PRACTICE |
| TPL-009 | LOW | `core/models.py::DataDict.fts_separator` | confirmed | BEST-PRACTICE |
| TPL-010 | LOW | `core/models.py::DataFile` | confirmed | BEST-PRACTICE |

**Tally:** confirmed 10 · re-graded 0 · re-typed 0 · merged 0 · not substantiated 0 · unsettled 0.

---

## Per-Finding Validation

### TPL-001 — Confirmed [HIGH] / SPEC-DEVIATION

**Block 1 — re-derived (ran):** `config_to_dict` (utils.py:144) catches
`(FileNotFoundError, ValueError)` and returns `{}`. Repro at commit: a malformed
`app_config.json5` (`{ this is : not valid ,,`) with a valid `log_config.json5` —
`get_app_service()` constructs `AppService` with no error (existence-only gate
`_require_settings`, app_service.py:316–323 passes); accessing `service.app_config`
raises `pydantic_core._pydantic_core.ValidationError` ("6 validation errors for
DataSettings … Field required"), **not** `ConfigError`. The parse error is logged
at ERROR only (`<string>:1 Unexpected "i" at column 8`) and never reaches the
exception.

**Block 2 — green control:** The only tests covering `config_to_dict` are
`test_config_to_dict_returns_empty_dict_on_invalid_json5` (test_app.py:250) and
`test_config_to_dict_returns_empty_dict_on_missing_file` (test_app.py:245) — both
PASSED. Both assert the **masking** behaviour (return `{}` on parse/missing). They are
not supporting controls for the absence of masking; they **are the masking**.

**Block 3 — grade vs Phase 05 rubric:** HIGH = "a malformed template passes the
existence gate, surfacing only an opaque error." Matches exactly. CRITICAL is
correctly *not* used (the gate raises on *absence*). The dead `except` is a separate
but linked defect — `raise DataValidationError` has 0 matches in `src/`/`tests/`
(verified); `DataValidationError(Exception)` (errors.py:12) is not a subclass of
`pydantic.ValidationError` (verified at runtime by Phase 02 VAL-001 evidence:
`issubclass → False`).

**Block 4 — side that moves:** CODE (utils.py `config_to_dict` + app_service.py
`app_config` exception handler) + DOC (`current-behavior.md:286` falsely claims a
`ConfigError` wrap). Code is load-bearing.

**Block 5 — carry-out:** Fix targets resolve: `utils.py::config_to_dict` (distinguish
missing→`{}` from parse-error→raise) and `app_service.py::AppService.app_config`
(catch `pydantic.ValidationError`). The two tests at test_app.py:245/250 must be
rewritten as part of the fix (documented as remediation blockers). No other code breaks
since the `except DataValidationError` is already dead.

**Block 6 — cross-phase:** Same root cause as `ENT-001` (merged→`CFG-001` per
Phase 01 validation). Phase 02 validation confirmed `CFG-001` independently. Both
reach the same conclusion (opaque `ValidationError`, not `ConfigError`). Not a
conflict. The Phase 05 ownership is the template↔model↔Polars conformance surface;
the `current-behavior.md:286` doc contradiction is Phase 05's distinct angle.

**Evidence (ran at c0fd0e1, polars 2.0.0, pydantic 2.12.5):**
```text
get_app_service succeeded (gate passed): AppService
ERROR - <string>:1 Unexpected "i" at column 8 - mko_data_cleaner.core.utils
app_config raised raw ValidationError (NOT ConfigError): 6 validation errors for DataSettings
```

### TPL-002 — Confirmed [MEDIUM] / SPEC-DEVIATION

**Block 1 — re-derived (ran):** `infer_schema_length: bool = False` (models.py:102).
Template value `"infer_schema_length": false` (app_config.json5:35). Polars 2.0 `read_csv`
expects `int | None`; `False`→`0`→zero-row inference → every column `String`. Numeric
`1000` is rejected by Pydantic (`type=bool_parsing`). Repro:

```text
infer_schema_length=False -> dtypes: {'a': 'String', 'b': 'String'}
infer_schema_length=0     -> dtypes: {'a': 'String', 'b': 'String'}
infer_schema_length=50    -> dtypes: {'a': 'Int64', 'b': 'String'}
PolarsReadCSV(infer_schema_length=1000) -> ValidationError: Input should be a valid boolean
```

**Block 2:** No green control pins the `bool` type — `test_app.py` sets `chunk_size`
explicitly (not `infer_schema_length`); no test asserts `bool` on this field.
Support is repro + code reading.

**Block 3 — grade:** Phase 05 MEDIUM = "model accepts a value the downstream consumer
rejects or treats differently." `bool` is treated by Polars as `0` (all-`String`).
Matches. HIGH is for silent masking/dropping (TPL-001) — not a type-contract drift.

**Block 4 — moves:** CODE (`models.py::PolarsReadCSV.infer_schema_length` type +
`app_config.json5:35` value). Load-bearing on both sides.

**Block 5 — carry-out:** `infer_schema_length: int | NonNegativeInt | None = None`;
template → `null` or a real integer. No other code breaks; `reader_settings` flows
through unchanged once the type is widened.

### TPL-003 — Confirmed [MEDIUM] / SPEC-DEVIATION

**Block 1 — re-derived (ran):** `compression: Literal["gzip", "bz2", "zip", "xz",
"zstd"] | None = "gzip"` (models.py:91). Polars 2.0 `DataFrame.write_csv` accepts
only `gzip`/`zstd`/`uncompressed`; `bz2`/`zip`/`xz` raise `InvalidOperationError`:

```text
write_csv compression gzip -> OK
write_csv compression zstd -> OK
write_csv compression bz2  -> REJECTED InvalidOperationError Invalid compression format: (bz2)
write_csv compression zip  -> REJECTED InvalidOperationError Invalid compression format: (zip)
write_csv compression xz   -> REJECTED InvalidOperationError Invalid compression format: (xz)
```

Shipped template uses `gzip` (safe) — latent, not active. No shipped test exercises
non-`gzip`. Support is repro + code reading.

**Block 3 — grade:** MEDIUM = "model accepts a value the downstream consumer rejects."
Matches.

**Block 4 — moves:** CODE (`models.py::PolarsWriteCSV.compression`).

**Block 5 — carry-out:** Narrow to `Literal["uncompressed", "gzip", "zstd"] | None`.
`CSVWorker.get_files_suffix` (csv_service.py:243–262, `match` at :251) handles
`bz2`/`zip`/`xz` cases — dropping them from the `Literal` leaves dead `case` arms;
flag for cleanup, not breakage.

### TPL-004 — Confirmed [MEDIUM] / BEST-PRACTICE

**Block 1 — re-derived (ran):** `include_bom` is absent from
`PolarsWriteCSV.model_fields` (False), yet `PolarsWriteCSV(include_bom=True).model_dump()`
returns `include_bom: True` (survives via `extra="allow"`). Template has
`"include_bom": true` (app_config.json5:41). Works only because Polars `write_csv`
happens to accept the param. Repro:

```text
"include_bom" in PolarsWriteCSV.model_fields -> False
PolarsWriteCSV(include_bom=True).model_dump()["include_bom"] -> True
```

**Block 2:** No green control. Support is repro + code reading.

**Block 3 — grade:** MEDIUM = "model accepts a value the downstream consumer rejects or
treats differently" — here the model *fails to type* a value (undeclared field), silently
absorbed. Fits the Phase 05 MEDIUM band as applied.

**Block 4 — moves:** CODE (`models.py::PolarsWriteCSV` — declare the field).

**Block 5 — carry-out:** `include_bom: bool = False` on `PolarsWriteCSV`.
Non-breaking; `model_dump()` still carries `include_bom` (now typed).

### TPL-005 — Confirmed [MEDIUM] / BEST-PRACTICE

**Block 1 — re-derived (ran):** Extra policies verified at runtime:

```text
DataSettings extra (unset -> ignore): None
DataSettings(**{**shipped,'bogus_xyz':1}) -> OK (bogus_xyz silently dropped)
PolarsWriteCSV extra: allow
PolarsWriteCSV(chunk_sz=100000).model_dump() -> {'chunk_size': 10000, 'chunk_sz': 100000}
params.pop("chunk_size", 10000) => 10000  (intended 100000 lost)
```

Per-model policies (c0fd0e1): `WorkingPaths`=forbid (models.py:62);
`DataFile`/`DataDict`/`Database`/`DictColumnsIndexes`/`PolarsReadCSV`/`PolarsWriteCSV`
=allow (models.py:79,155,47,147,95,86); `DataSettings`/`LoggingSettings`=unset→ignore
(models.py:170,52).

**Block 2:** No green control pins the typo-absorption behaviour. Support is repro +
code reading.

**Block 3 — grade:** Phase 05 MEDIUM = "model accepts a value the downstream consumer
rejects or treats differently." The Phase 05 *phase file* explicitly lists
"extra-policy inconsistency" under MEDIUM (05-audit-config-templates.md:140). The
input's "Why this band" quotes the band text "a config value silently
absorbed/dropped so intent is unmet with no error" — that exact phrase also appears in
the Phase 05 **HIGH** band (line 132). Grading holds: the Phase 05 phase file places the
extra-policy inconsistency under MEDIUM; the bogus-key-drop "symptom" is noted as
"Related, not filed" under HIGH (05-audit-config-templates.md:136). No re-grade.

**Block 4 — moves:** CODE (`models.py` — single `extra` policy, prefer `forbid` on
`DataSettings`/`LoggingSettings`).

**Block 5 — carry-out:** Targets resolve. Rollout risk: `extra="forbid"` on
`DataSettings` may reject existing user configs relying on absorbed keys — gate behind
release note + optional migrate (the input's Fix Order notes this).

### TPL-006 — Confirmed [MEDIUM] / BEST-PRACTICE

**Block 1 — re-derived (ran):** Three definitions of `chunk_size`:

```text
Model default (PolarsWriteCSV()):    chunk_size = 10000   (models.py:90)
Template (app_config.json5):         chunk_size = 100000  (app_config.json5:40)
Code fallback (export_sql_to_csv):   params.pop("chunk_size", 10000)  (csv_service.py:315)
```

`CSVWorker.CHUNK_SIZE = 10000` (csv_service.py:28) is the read-scan batch size
(`collect_batches(chunk_size=self.CHUNK_SIZE)`, csv_service.py:180) — a separate
concern, correctly identified as such.

**Block 2:** No green control — `test_csv_service.py` sets `chunk_size` explicitly
(e.g. `1`). Support is runtime dump + static reading.

**Block 3 — grade:** MEDIUM = "default-value divergence across template/model/code."
Matches Phase 05 MEDIUM.

**Block 4 — moves:** CODE (`models.py::PolarsWriteCSV.chunk_size` +
`csv_service.py::CSVWorker.export_sql_to_csv` remove `pop` fallback).

**Block 5 — carry-out:** Make the model default the single source; remove the redundant
`pop(..., 10000)` (the key is always present after `model_dump()`). No shipped test
pins a particular default.

### TPL-007 — Confirmed [LOW] / BEST-PRACTICE

**Block 1 — re-derived (ran):** `DataFileExtension` (models.py:72–74) declares
`default = "csv"` and `csv = "csv"`. Runtime:

```text
aliases: [('default', 'csv'), ('gz', 'gz')]
csv == default? True   |   csv.value: csv
```

`csv` is an alias, not a distinct member.

**Block 2:** No green control. `test_app.py:224` references `DataFileExtension.gz`
only — never `.csv`. Support is repro + code reading.

**Block 3 — grade:** LOW = "doc/cosmetic drift." Matches Phase 05 LOW.

**Block 4 — moves:** CODE (`models.py::DataFileExtension`).

**Block 5 — carry-out:** Drop `csv` alias (or make `csv` canonical). No consumer
reference `DataFileExtension.csv`.

### TPL-008 — Confirmed [LOW] / BEST-PRACTICE

**Block 1 — re-derived (ran + static):** `log_config.json5:45` defines
`loggers.__main__`. No module creates a `__main__` logger — every source module uses
`getLogger(__name__)` or `getLogger("app_service")` (→ `mko_data_cleaner.*`).
`grep "__main__" src/*.py` yields only `if __name__ == "__main__":` execution guards
(app.py:153, app_service.py:343, init_service.py:56) — none calls `getLogger("__main_")`.
`app.py` does **not** import `logging` at all. CLI entry is the Typer console script
`mko-data-cleaner = "mko_data_cleaner.app:app"` (pyproject.toml:45), not
`python app.py`. The `__main__` logger is inert.

**Block 2:** No green control. Support is grep + pyproject reading.

**Block 3 — grade:** LOW = "dead config." Matches Phase 05 LOW.

**Block 4 — moves:** TEMPLATE (`settings/log_config.json5` — remove the
`loggers.__main__` block). Phase 02 owns bootstrap confirmation.

**Block 5 — carry-out:** Remove the block; `dictConfig` receives the template dump.
No code reads `__main__` from config.

### TPL-009 — Confirmed [LOW] / BEST-PRACTICE

**Block 1 — re-derived (ran + static):** `fts_separator` (models.py:159,
`fts_separator: str = "|"`) and template value `"fts_separator": "|"` (app_config.json5:16).
`grep -rn fts_separator src/mko_data_cleaner/*.py` → only models.py:159 (the
declaration). `_build_fts_query` (dict_service.py:231) uses a hardcoded
`separator="|"` parameter default — `fts_separator` does not appear in its source.
`build_match_query` (dict_service.py:264) uses no separator. Zero consumers read the
config field.

**Block 2:** No green control. Support is grep + code reading.

**Block 3 — grade:** LOW = "declared-but-unread." Matches Phase 05 LOW.

**Block 4 — moves:** CODE+CONFIG (`dict_service.py` wire it in, or remove field +
template key). Phase 02 owns the FTS path.

**Block 5 — carry-out:** `dict_file_settings.extension` *is* read (csv_service.py:211,
`self.dict_settings.get("extension", …)` in `get_merged_dictionary`) — the input's
auditor correction is accurate; only `fts_separator` is dead.

### TPL-010 — Confirmed [LOW] / BEST-PRACTICE

**Block 1 — re-derived (ran + static):** `index_column: str = None` (models.py:81)
and `date_column: str = None` (models.py:82). `DataFile(extension='csv')` yields
`index_column=None, date_column=None` — `None` despite the `str` annotation.
`basedpyright` (standard) flags exactly these two lines:

```text
models.py:81:25 - error: Type "None" is not assignable to declared type "str"
models.py:82:24 - error: Type "None" is not assignable to declared type "str"
```

**Block 2:** No green control pins the `str` type. `test_app.py:222` asserts
`index_column == "AdId"` (a str) — annotating `str | None` keeps that green. Support
is repro + basedpyright.

**Block 3 — grade:** LOW = "type-discipline drift." Matches Phase 05 LOW.

**Block 4 — moves:** CODE (`models.py::DataFile`).

**Block 5 — carry-out:** `index_column: str | None = None` /
`date_column: str | None = None`. Non-breaking; `test_app.py:222` stays green.

---

## Cross-Finding Analysis

### Cross-phase: Phase 05 ↔ Phase 01 ↔ Phase 02

| Phase-05 finding | Phase-01 finding | Phase-02 finding | Relationship |
|---|---|---|---|
| TPL-001 (HIGH) malformed config mask + dead `except DataValidationError` | ENT-001 (HIGH) | CFG-001 (HIGH) | **Cross-reference.** All three conclude: `config_to_dict` masks parse errors as `{}`; the `except DataValidationError` at app_service.py:51 is dead (never raised). Phase 01 validation merged ENT-001→CFG-001. TPL-001 adds the Phase 05 angle: `current-behavior.md:286` doc-code contradiction + template-level masking. Same verdict → not a conflict. |

No opposite conclusions. TPL-001's distinct Phase-05 contribution is the
`current-behavior.md:286` doc claim ("Pydantic validation error wrapped in ConfigError")
which the code contradicts — a DOC-UPDATE angle layered onto the same root cause.

### Merge candidates (within Phase 05)

- **TPL-002 + TPL-003 + TPL-004 + TPL-006** — "model fields drift from consumer
  contract." One coordinated pass on `models.py` + `app_config.json5`. The input's
  Fix Order correctly identifies this cluster.
- **TPL-004 + TPL-005** — both concern `extra="allow"` silent absorption. TPL-005 is
  the policy root; TPL-004 is a symptom.
- **TPL-007 + TPL-010** — `DataFile` type/enum discipline (both Phase 10-owned).

### Conflicting evidence

None. All repros matched the input's evidence fences. The input's Appendix A
corrections (YAML→JSON5, `pandas` comment, `models.py:102` type,
`dict_file_settings.extension` liveness) were independently verified at c0fd0e1.

---

## Rollout Safety

| Finding | What the fix breaks | Test that must stay green |
|---|---|---|
| TPL-001 | `config_to_dict` returns `{}` on parse error; exception type changes | `test_config_to_dict_returns_empty_dict_on_invalid_json5` (test_app.py:250) and `test_config_to_dict_returns_empty_dict_on_missing_file` (test_app.py:245) must be rewritten to distinguish missing→`{}` from parse-error→raise |
| TPL-002 | Widens `infer_schema_length` type; template value changes from `false` to `null`/int | No shipped test pins `bool`; `test_csv_service.py` sets reader settings explicitly |
| TPL-003 | Narrows `compression` Literal; `get_files_suffix` dead `case` arms | `test_get_files_suffix` (test_csv_service.py) — must still pass for `gzip`/`gz` |
| TPL-004 | Declares `include_bom` as a typed field | No shipped test asserts `include_bom`; `export_sql_to_csv_gzip` (test_csv_service.py) stays green |
| TPL-005 | `extra="forbid"` on `DataSettings`/`LoggingSettings` rejects absorbed keys | Existing tests use valid shipped config (no bogus keys); `test_app.py` all green. Gate behind release note for user configs relying on absorbed keys |
| TPL-006 | Removes `params.pop("chunk_size", 10000)` fallback | `test_csv_service.py` sets `chunk_size` explicitly (e.g. `1`) — all stay green |
| TPL-007–010 | Enum/field changes | `test_app.py:224` (`.gz`) and `:222` (`index_column=="AdId"`) stay green; `str | None` keeps both |

All fix targets resolve in `git ls-files` at c0fd0e1 (16/16 verified).

---

## VAL Findings

| ID | Severity | Subject | Verdict |
|---|---|---|---|
| VAL-001 | LOW | Findings summary says "Four are MEDIUM" but lists 5 MEDIUM findings (TPL-002–TPL-006). Counting/wording error in the summary narrative only; the per-finding table is correct (5 MEDIUM, 4 LOW, 1 HIGH, 10 total) | Advisory; no finding affected |
| VAL-002 | LOW | TPL-005 "Why this band" quotes "a config value silently absorbed/dropped so intent is unmet with no error" attributed to Phase 05 MEDIUM, but that text appears in the Phase 05 **HIGH** band (05-audit-config-templates.md:132–133). The Phase 05 phase file explicitly lists "extra-policy inconsistency" under MEDIUM (line 140), so the filing is defensible; only the citation is inaccurate | Advisory; finding remains confirmed |
| VAL-003 | LOW | TPL-003 fix cites `CSVWorker.get_files_suffix` at `csv_service.py:251` — line 251 is the `match method:` statement inside the method (def at :243), a valid in-method anchor but not the definition line | Advisory; anchor resolves; no impact on validity |

No validation-level findings affect the substantiation of any finding.

---

## Appendices

### Appendix A — Reproduction evidence (consolidated, ran at c0fd0e1)

**Baseline tooling:**
```text
$ git rev-parse --short HEAD        ->  c0fd0e1
$ uv run ruff check src/ tests/     ->  All checks passed!
$ uv run basedpyright src/          ->  39 errors, 0 warnings, 0 notes
$ uv run pytest                     ->  93 passed in 3.84s
$ python -c "import pydantic; print(pydantic.VERSION)"  ->  2.12.5
$ python -c "import polars; print(polars.__version__)"  ->  2.0.0
```

**TPL-001 — malformed config passes gate, raises raw ValidationError:**
```text
get_app_service succeeded (gate passed): AppService
ERROR - <string>:1 Unexpected "i" at column 8 - mko_data_cleaner.core.utils
app_config raised raw ValidationError (NOT ConfigError): 6 validation errors for DataSettings
```

**TPL-002 — infer_schema_length bool→all-String; numeric rejected:**
```text
infer_schema_length=False -> dtypes: {'a': 'String', 'b': 'String'}
infer_schema_length=0     -> dtypes: {'a': 'String', 'b': 'String'}
infer_schema_length=50    -> dtypes: {'a': 'Int64', 'b': 'String'}
PolarsReadCSV(infer_schema_length=1000) -> ValidationError: Input should be a valid boolean
```

**TPL-003 — compression Literal vs Polars 2.0 write_csv:**
```text
write_csv compression gzip     -> OK
write_csv compression zstd     -> OK
write_csv compression uncompressed -> OK
write_csv compression bz2      -> REJECTED InvalidOperationError Invalid compression format: (bz2)
write_csv compression zip      -> REJECTED InvalidOperationError Invalid compression format: (zip)
write_csv compression xz       -> REJECTED InvalidOperationError Invalid compression format: (xz)
```

**TPL-004 — include_bom undeclared, survives via extra="allow":**
```text
"include_bom" in PolarsWriteCSV.model_fields -> False
PolarsWriteCSV(include_bom=True).model_dump()["include_bom"] -> True
```

**TPL-005 — extra policy absorption + chunk_sz typo:**
```text
DataSettings extra (unset -> ignore): None
DataSettings(**{**shipped,'bogus_xyz':1}) -> OK (bogus_xyz silently dropped)
PolarsWriteCSV(chunk_sz=100000).model_dump() -> {'chunk_size': 10000, 'chunk_sz': 100000}
params.pop("chunk_size", 10000) => 10000  (intended 100000 lost)
```

**TPL-006 — chunk_size three-site divergence:**
```text
Model default (PolarsWriteCSV()):    chunk_size = 10000   (models.py:90)
Template (app_config.json5:40):      chunk_size = 100000
Code fallback (csv_service.py:315):  params.pop("chunk_size", 10000)
CSVWorker.CHUNK_SIZE = 10000         (csv_service.py:28, read-scan only)
```

**TPL-007 — DataFileExtension csv alias:**
```text
members: [('default', 'csv'), ('gz', 'gz')]   (csv is an alias, not a member)
csv == default? True
```

**TPL-008 — __main__ logger inert:**
```text
grep "__main__" src/mko_data_cleaner/*.py -> only if __name__ == "__main__" guards
grep "getLogger" app.py -> (no matches; app.py does not import logging)
pyproject.toml:45: mko-data-cleaner = "mko_data_cleaner.app:app"
```

**TPL-009 — fts_separator never read:**
```text
grep fts_separator src/mko_data_cleaner/*.py -> only models.py:159 (declaration)
_build_fts_query separator param: default="|"  (hardcoded; config field not read)
```

**TPL-010 — str = None type drift:**
```text
DataFile(extension='csv').model_dump() -> {'index_column': None, 'date_column': None}
basedpyright: models.py:81:25 - error: Type "None" is not assignable to declared type "str"
            models.py:82:24 - error: Type "None" is not assignable to declared type "str"
```

**TPL-001 dead-except confirmation:**
```text
grep -rn "raise DataValidationError" src tests -> (0 matches)
DataValidationError(Exception) is not a subclass of pydantic.ValidationError -> False
```

### Appendix B — All cited paths tracked (git ls-files)

All 16 cited source/doc/test files verified tracked at c0fd0e1:
`utils.py`, `app_service.py`, `models.py`, `csv_service.py`, `dict_service.py`,
`errors.py`, `paths.py`, `init_service.py`, `db_service.py`, `app.py`,
`app_config.json5`, `log_config.json5`, `test_app.py`, `current-behavior.md`,
`open-questions.md`, `pyproject.toml`.

### Appendix C — Template artefact (Block 8)

Front matter present and honest in the input: `phase`, `phase_name`, `id_prefix`,
`mode`, `report_status`, `severity_taxonomy`, `code_ref`. Per-finding template fields
(Anchor, Disambiguator, Checked, Why this band, Problem, Repro, Impact, Root cause,
Fix, Verify, Related, Evidence) survive into all 10 findings. Severity-rubric pointer
resolves: `.kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy`
→ Phase 05 file §`## Severity Taxonomy` (line 125). No `:<digits>` locators in
anchor fields. Every "ran" claim has its output fence below it.

### Appendix D — Namespace integrity (Block 7)

- Declared prefix: `TPL-` (findings.md front matter + Phase 05 phase file)
- Identifiers minted: TPL-001…TPL-010 (10, sequential)
- In-source `TPL-` provenance markers: none (findings are external to the code;
  no embedded `# TPL-NNN` annotations in source/config/tests)
- Compound form: `TPL-NNN` — resolves uniquely
- Reuse across phases: `ENT-` (Phase 01), `CFG-` (Phase 02), `TPL-` (Phase 05) —
  all disjoint, no collisions
- **Ruling:** Phase 05 correctly reuses the declared `TPL-` namespace; no second
  namespace minted; no in-source markers to migrate or retire.
