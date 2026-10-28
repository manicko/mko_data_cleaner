# 04 — Uninformative error on dictionary structure mismatch (`IndexError: index 1 is out of bounds for sequence of length 1`)

- **Status:** ✅ Investigation complete. Root cause identified and reproduced. Error chain verified. Ready for task creation.
- **Date:** 2026-10-26
- **Source request:** user report — *"Ошибка: index 1 is out of bounds for sequence of length 1. явно ошибка в данных, но описание очень неконконкретное, нет информации - на каком шаге, в каком файле ничего."*
- **Evidence base:** repository inspection, runtime reproduction with the user's real data sample (`.ai/tmp/gac/`), Polars 2.0.0 error-trace verification.

---

## 1. Problem statement

When a user runs `mko-data-cleaner run <report_dir>` (or calls `process_data(...)`) on a
report whose **dictionary file** has a **CSV separator that does not match** the
`read_settings.from_csv.separator` configured in `app_config.json5`, Polars silently reads
the dictionary as a **single-column** DataFrame. Downstream, `MappingDict._initialize_mapping()`
attempts to select columns by index `[0, 1, 2, 4]` (the values of `col_indexes`) from that
1-column frame. Polars raises:

```
IndexError: index 1 is out of bounds for sequence of length 1
```

This raw `IndexError` propagates all the way to `app.py`'s catch-all handler and is displayed
to the user as:

```
❌ Ошибка: index 1 is out of bounds for sequence of length 1
```

The message contains **zero actionable context**: no file path, no step name, no hint about
the separator mismatch, no indication that the dictionary structure is the problem.

## 2. Root cause

### The triggering data

The user's dictionary file `dict/merged_dictionary.csv` uses **comma** `,` as the field
separator:

```csv
action,match,search_column_idx,value,term,articleLevel4Name_clean,...
r,p,2,ЛЕГКОВЫЕ АВТОМОБИЛИ,ЛЕГКОВЫЕ АВТОМОБИЛИ,...
```

However, the bundled `app_config.json5` template specifies:

```json5
"read_settings": {
    "from_csv": {
        "separator": ";",       // ← semicolon!
        ...
    }
}
```

### What happens at runtime

1. `CSVWorker.get_dictionary()` (csv_service.py:203-206) calls
   `pl.read_csv(self.dict_path, **self.reader_settings)`.
2. Because the file uses `,` but the reader expects `;`, Polars treats each entire
   line as a **single field**. The result is a DataFrame with **1 column** whose name is
   the entire header line. This succeeds silently — no error.
3. `MappingDict.__init__` (dict_service.py:17) receives this 1-column DataFrame.
4. `_initialize_mapping()` (dict_service.py:96-114) executes:
   ```python
   action_names = list(self.action_col_indexes.keys())   # ["action", "match", "search", "term"]
   action_idx   = list(self.action_col_indexes.values())  # [0, 1, 2, 4]
   ...
   self._data_actions = self.data[:, action_idx]          # ← line 107, CRASH
   ```
5. `self.data[:, [0, 1, 2, 4]]` on a 1-column DataFrame → Polars internally calls
   `df.to_series(1)` → `IndexError: index 1 is out of bounds for sequence of length 1`

### Verified reproduction

The error was reproduced with a minimal script against the user's real data:

```python
>>> df = pl.read_csv(dict_path, separator=';')
>>> df.width
1
>>> MappingDict(data=df, action_col_indexes=DictColumnsIndexes(action=0, match=1, search=2, term=4))
IndexError: index 1 is out of bounds for sequence of length 1
```

With the correct separator (`','`), the dictionary reads 9 columns and everything works.

## 3. Full error propagation chain

| Frame | File:Line | What happens |
|---|---|---|
| 1 | `app.py:80` | `process_data(...)` → `service.run_report(report)` |
| 2 | `app_service.py:138` | `MappingDict(data=df, action_col_indexes=...)` constructed |
| 3 | `dict_service.py:31` | `__init__` calls `self._initialize_mapping()` |
| 4 | `dict_service.py:107` | `self._data_actions = self.data[:, action_idx]` |
| 5 | `polars/dataframe/frame.py:1412` | `df.__getitem__` → `get_df_item_by_key` → `_select_columns_by_index` |
| 6 | `polars/dataframe/frame.py:2689` | `self._df.to_series(1)` → **`IndexError`** |
| 7 | back to `dict_service.py:31` | IndexError propagates out of `__init__` |
| 8 | `app_service.py:110` | `run_report` propagates (no `try/except` around MappingDict construction) |
| 9 | `app.py:87` | `except Exception as e: console.print(f"❌ Ошибка: {e}")` |

The user sees:
```
❌ Ошибка: index 1 is out of bounds for sequence of length 1
```

**No traceback is shown to the CLI user.** The `logger.debug(f"…")` calls in `app_service`
and the `logger.error(traceback.format_exc())` calls in `csv_service.get_dictionary` (line 85)
do not fire because `get_dictionary()` succeeds — the error occurs in `MappingDict.__init__`,
which has **no try/except at all**. The internal log output (if the user knew to look) would
show the traceback, but the console output is just the bare `IndexError` message.

## 4. Why this is a broader class of problem

This is not an isolated incident. The codebase has **no structural validation** of the
dictionary file. The `col_indexes` configuration is trusted blindly:

- `_initialize_mapping()` (dict_service.py:96-114) assumes `self.data` has at least
  `max(action_idx) + 1` columns — **no bound check**.
- `get_dictionary()` (csv_service.py:203-206) reads the file and returns whatever Polars
  produces — **no validation** that the expected columns exist.
- `build_mapping()` (dict_service.py:119-142) assumes `self.data_col_index` is not `None` —
  if `_table_columns` is empty, `_get_data_col_index` returns `None` and a later
  `TypeError` would occur.
- `build_match_query()` (dict_service.py:264-278) does `columns_list[col_idx]` on a plain
  Python list — this raises `IndexError: list index out of range` (a *different* message)
  when the dictionary references a data-column index beyond the available data columns.

There are **at least 4 distinct error messages** that can arise from the same class of
"dictionary/data column mismatch" bug:
1. `IndexError: index 1 is out of bounds for sequence of length 1` (Polars, column selection)
2. `IndexError: list index out of range` (Python list, in `build_match_query`)
3. `ColumnNotFoundError` (Polars, in `_build_query` join)
4. `KeyError` or `TypeError` (when `data_col_index` is `None`)

None of these messages identify the file, the step, or the mismatch.

## 5. Missing error-handling patterns

### Current state of error handling

| Layer | Pattern | Quality |
|---|---|---|
| `app.py` (CLI `run`) | `except Exception as e: print(f"❌ Ошибка: {e}")` | **Bare message, no context** |
| `app_service.py` (run_report) | No try/except around `MappingDict` construction | Errors propagate raw |
| `dict_service.py` (`get_data_mapping_by_action`) | `except Exception as err: logger.error(err); raise err` | Logs but re-raises same exception |
| `dict_service.py` (`_initialize_mapping`) | **No try/except at all** | Raw Polars error surfaces |
| `csv_service.py` (`get_csv_headers`, `_read_csv_in_chunks`, `get_merged_dictionary`, `export_sql_to_csv`) | `except Exception as err: logger.error(traceback.format_exc()); raise err` | Logs traceback internally but re-raises bare exception |
| `db_service.py` (`perform_query`) | `except sqlite3.Error as err: ...` with query context in log, re-raises | SQL context available in logs only |
| `app_service.py` (`app_config` property) | `except DataValidationError as e: raise ConfigError(f"Invalid config: {e}")` | **Good** — wraps with context |

Only `ConfigError` wrapping sets a positive example. The rest either log internally and
re-raise the original, or catch nothing at all.

## 6. Proposed solution directions

### A. Add a dedicated exception type
Add `DictionaryStructureError(Exception)` to `errors.py` that carries:
- The dictionary file path
- Expected minimum column count (from `col_indexes`)
- Actual column count
- Actual column names

### B. Validate early in `MappingDict.__init__` / `_initialize_mapping`
Before selecting columns by index, check:
```python
max_idx = max(self.action_col_indexes.values())
if self.data.width <= max_idx:
    raise DictionaryStructureError(
        file_path=<dict_path>,
        expected_min_columns=max_idx + 1,
        actual_columns=self.data.columns,
    )
```
Additionally: detect the likely separator mismatch by warning when `data.width == 1`
(the signature of a wrong separator) with a remediation hint.

### C. Validate at the `CSVWorker.get_dictionary()` boundary
After reading, validate that the loaded DataFrame has at least `max(col_indexes.values()) + 1`
columns before returning. This catches the error earlier and closer to the I/O boundary.

### D. Add step context in `app.py` error handler
Wrap the `run` command body so that the console message includes:
- The pipeline step (e.g., "dictionary initialization")
- The file path involved
- The underlying error type

Instead of `❌ Ошибка: index 1 is out of bounds for sequence of length 1`, the user would
see something like:
```
❌ Ошибка: Dictionary structure validation failed
  File: /path/to/report/dict/merged_dictionary.csv
  Step: Building mapping rules
  Issue: Dictionary has 1 column(s) but col_indexes references column index 4.
  Hint: Check if the dictionary CSV separator matches read_settings.from_csv.separator
  (configured: ";", dictionary appears to use: ",")
```

### E. Improve `build_match_query` bounds checking
Guard `columns_list[col_idx]` against index errors with a clear message when the
dictionary references a data-column index that doesn't exist in the table.

## 7. Affected files

| File | Change |
|---|---|
| `src/mko_data_cleaner/core/errors.py` | Add `DictionaryStructureError` |
| `src/mko_data_cleaner/core/dict_service.py` | Add validation in `_initialize_mapping`; guard `build_match_query` |
| `src/mko_data_cleaner/core/csv_service.py` | Add validation in `get_dictionary` |
| `src/mko_data_cleaner/app.py` | Add step-context wrapper around `run` error handling |

## 8. Test strategy

- **Unit test**: `MappingDict` with a 1-column DataFrame raises `DictionaryStructureError`
  (not bare `IndexError`) with a message containing the file path and expected/actual counts.
- **Unit test**: `build_match_query` with an out-of-range `col_idx` raises a descriptive error.
- **Unit test**: `CSVWorker.get_dictionary` with a separator mismatch raises a descriptive error.
- **Integration test**: `process_data` on the `.ai/tmp/gac` sample raises a catchable
  exception with a message that mentions the dictionary file and the separator mismatch.

## 9. Out of scope

- Changing the dictionary file format (the comma separator is the user's data, not a bug
  in the code — the tool should adapt or give a clear error).
- Auto-detecting and fixing the separator automatically (could mask other data issues;
  a clear error is preferred).
- Changing the error displayed to a fully Russian or fully English message (see
  [open-questions.md](docs/00-overview/open-questions.md) for the i18n discussion).
