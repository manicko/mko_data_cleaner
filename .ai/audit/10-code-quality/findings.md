---
phase: "10"
phase_name: "code-quality"
id_prefix: "QLT-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/10-audit-code-quality.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Code Quality

## Summary

Seven findings, highest severity **MEDIUM**. The audit's largest surface is
convention and consistency hygiene across the constant vocabulary and the
project's own rules: a control-flow/identifier regex maintained as two
independent literals, a dead second copy of the action-type enum with zero
readers, an undocumented col-index numbering gap whose consequence is masked by
the test fixture, Russian text in non-user-facing internal comments/docstring,
commented-out debug `print`s, and docstrings that contradict their own
behavior. The type-checker config declines one diagnostic class with no recorded
rationale; the in-code suppression surface is otherwise minimal and clean.

## Scope

**Examined:** `core/utils.py` (`ALLOWED_PATTERN`, `is_valid_name`, `clean_names`,
`progress_bar`, `config_to_dict`, `merge_dicts`), `core/models.py` (`NameConstrained`,
`ActionType`, `MatchType`, `MappingColumns`, `DictColumnsIndexes`, `DataFileExtension`,
`DataFile`, `PolarsReadCSV`/`PolarsWriteCSV`, `DataSettings`), `core/db_service.py`
(`DBWorker.AVAILABLE_ACTIONS`, `create_table_with_index`, `_create_index`,
`_initialize_mapping`-adjacent SQL, `_ensure_pyarrow_importable`, commented debug),
`core/dict_service.py` (`MappingDict._initialize_mapping`, commented debug),
`core/app_service.py` (`run_report` print() surface — filed in phase 01),
`app.py` (`__all__`, module docstring, typer command surfaces), `pyproject.toml`
`[tool.ruff]`/`[tool.basedpyright]` config.

**Tools:** `uv run ruff check src/ tests/` (clean), `uv run basedpyright src/`
(39 errors, 0 warnings — counted, not re-listed per finding; type-error details
are owned by phase 04), `uv run pytest` (93 passed), `uv run python` repros,
`git ls-files`, `grep`.

**Not verified:** the rendered/CLI surface language (phase 16); SQL-injection
posture at the persistence boundary (phase 12 — column names are routed through
`clean_names`/`validate_names` ahead of f-string interpolation); the real
production dictionary's column layout (the shipped fixture masks the col-index
gap — see QLT-002).

<!-- GATES — run before appending; each falsifies a fabricated finding:
     1. every cited path appears in `git ls-files`  ✓ (verified, see Appendix A)
     2. the symbol exists in that file, and the disambiguator token exists inside it
     3. no `:<digits>` appears in any locator field  ✓ (locators use ::Symbol/"token")
     4. every "ran"/static claim has its output pasted in an evidence fence below -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| QLT-001 | MEDIUM | `core/utils.py::ALLOWED_PATTERN` / `core/models.py::NameConstrained` | Identifier-validation regex lives in two homes; type system cannot enforce they agree |
| QLT-002 | MEDIUM | `core/models.py::DictColumnsIndexes` / `core/dict_service.py::MappingDict._initialize_mapping` | col-index numbering skips position 3 with no rationale; column 3 is silently dropped |
| QLT-003 | LOW | `core/db_service.py::DBWorker.AVAILABLE_ACTIONS` | Dead second copy of `ActionType` StrEnum; zero readers |
| QLT-004 | LOW | `core/db_service.py::"восстановим после clean_names"` / `app.py::"основной публичный интерфейс"` | Russian text in non-user-facing internal comments and module docstring |
| QLT-005 | LOW | `core/db_service.py::"# print(query)"` / `core/dict_service.py::"# print(df_result...)"` | Commented-out dead `print()` debug calls left in production modules |
| QLT-006 | LOW | `core/utils.py::is_valid_name` / `core/utils.py::clean_names` | Docstrings contradict behavior (list support rejected; "check" mislabels "clean") |
| QLT-007 | LOW | `pyproject.toml::"[tool.basedpyright]"` | `reportMissingTypeStubs="none"` declined with no recorded rationale |

<!-- Sorted by severity; index order is work order. -->

#### QLT-001 — [MEDIUM] Identifier-validation regex lives in two homes; type system cannot enforce they agree

**Anchor:** `src/mko_data_cleaner/core/utils.py::ALLOWED_PATTERN` · `src/mko_data_cleaner/core/models.py::NameConstrained`
**Disambiguator:** `^[a-zA-Z_][a-zA-Z0-9_]*$`
**Checked:** ran + static
**Why this band:** taxonomy MEDIUM = "a duplicated helper with no divergence yet"; the cause is a value with two homes where the type system cannot express that the two must agree (one is a bare `str` constant defaulted into `is_valid_name`, the other a Pydantic `Annotated` `StringConstraints` pattern — neither references the other). CRITICAL/HIGH are not used: both sites currently agree and nothing branches on their divergence yet.
**Problem:** The identifier-name rule `^[A-Za-z_][A-Za-z0-9_]*$` is encoded twice as independent literals: `ALLOWED_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$"` (`utils.py:15`, consumed by `is_valid_name`/`validate_names` ahead of SQL object creation in `db_service`) and `NameConstrained = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]` (`models.py:30-37`, consumed by `TableModel.table_name`/`column_name` and `Database.table_name`). They are semantically identical today, but a maintainer tightening one (e.g. length cap, Unicode rule) cannot discover the other from either site, and Pydantic/the `str` const give the type checker no way to compare them. `is_valid_name` runs at the DBWorker boundary while `NameConstrained` runs at the settings-model boundary — a divergence silently splits name accept/reject across layers.
**Repro:** `rg -n 'ALLOWED_PATTERN|NameConstrained' src/` finds both literals (Appendix B); runtime: `re`-compiling each and matching `["Ab_id","_a1","1bad","a-b","a b"]` agrees today (`== True`) but the two patterns are distinct string literals with no shared symbol (Appendix B).
**Impact:** On a future change to the identifier rule, one validation surface diverges from the other, producing names that pass sanitization in `clean_names` but fail `NameConstrained` (or vice-versa) — inconsistent DB-object naming with no test gate to catch it.
**Root cause:** No single named vocabulary for the identifier pattern; the rule was copy-pasted into a Pydantic `Annotated` rather than referenced.
**Fix:** Extract the pattern into one symbol (e.g. a module-level `IDENTIFIER_PATTERN: str` in `utils.py`) and have `NameConstrained` reference it (`pattern=IDENTIFIER_PATTERN`) so one literal is the only editable site. Target: `utils.py:15` + `models.py:33`.
**Verify:** new test needed: `test_identifier_pattern_single_source_of_truth` (asserts `NameConstrained`'s compiled pattern equals `ALLOWED_PATTERN` and they are not textual duplicates at two sites).
**Related:** Block 1, Block 6.

#### QLT-002 — [MEDIUM] col-index numbering skips position 3 with no rationale; column 3 is silently dropped

**Anchor:** `src/mko_data_cleaner/core/models.py::DictColumnsIndexes` · `src/mko_data_cleaner/core/dict_service.py::MappingDict._initialize_mapping`
**Disambiguator:** `term: NonNegativeInt = Field(default=4)`
**Checked:** ran + static
**Why this band:** taxonomy MEDIUM = "a constant surface whose conventions are undocumented" (and a bounded, masked correctness gap). The col-index numbering `0,1,2,4` has no documented rationale and `_initialize_mapping`'s `max()+1` derivation silently drops position 3; the consequence is currently masked (the shipped `sample_dictionary` fixture is always `NULL` at index 3), so it is not CRITICAL. HIGH is not used: the gap has not yet corrupted a real column.
**Problem:** `DictColumnsIndexes` defaults `action=0, match=1, search=2, term=4` (`models.py:148-151`) and the shipped template (`app_config.json5:17-21`) mirrors those values, but the gap at index 3 is never commented in either the model or the template. `_initialize_mapping` (`dict_service.py:96-114`) builds `action_idx = list(self.action_col_indexes.values())` → `[0,1,2,4]`, then `data_start_idx = max(action_idx) + 1` → `5`, and selects `self._data_actions = self.data[:, action_idx]` (columns 0,1,2,4) and `self._data_mapping = self.data[:, range(5, width)]`. Column position 3 belongs to neither set and is silently discarded; nothing warns if a real mapping dictionary carries a meaningful column there.
**Repro:** runtime — `DictColumnsIndexes().model_dump()` → `{'action':0,'match':1,'search':2,'term':4}`; sorted values `[0,1,2,4]`; gap `[3]`; `_initialize_mapping` excludes index 3 from both action and mapping selection (Appendix D). The `sample_dictionary` fixture (conftest.py:49-64) places `None` at index 3, which is why the drop is invisible in tests.
**Impact:** If a real mapping dictionary has a non-null column at position 3 (e.g. a `value` column the author intended to keep), its contents are dropped with no error or log — a silent data-loss path waiting for a real dictionary.
**Root cause:** The positional meaning of the col-index numbers is undocumented; the `max()+1` derivation treats the gap as "data starts right after the last action," swallowing index 3.
**Fix:** Either (a) document the gap (e.g. a comment stating index 3 is intentionally reserved/skipped) on `DictColumnsIndexes` and the template, or (b) stop deriving `data_start_idx` from `max(action_idx)` and instead enumerate the mapping/data range explicitly so a skipped position is an explicit, validated choice. Target: `models.py:148-151` + `dict_service.py:103-104`.
**Verify:** new test needed: `test_col_indexes_gap_is_not_silently_dropped` (build a 6-column dictionary with a non-null column at index 3 and assert it is either consumed or raises a documented error). No shipped test asserts the current gap or the drop.
**Related:** Block 2.

#### QLT-003 — [LOW] Dead second copy of `ActionType` StrEnum; `AVAILABLE_ACTIONS` has zero readers

**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker.AVAILABLE_ACTIONS`
**Disambiguator:** `"DELETE": "d"`
**Checked:** static (grep + source)
**Why this band:** taxonomy LOW = "a declared rule no caller reaches"; `AVAILABLE_ACTIONS` duplicates the `ActionType` StrEnum (`models.py:118-121`) and is never read, so it cannot diverge yet — it is pure hygiene drift on a constant surface (Block 2).
**Problem:** `DBWorker.AVAILABLE_ACTIONS = {"DELETE": "d", "REPLACE": "r", "ADD": "a"}` (`db_service.py:64-68`) is a literal re-statement of `ActionType(StrEnum): ADD="a", REPLACE="r", DELETE="d"` (`models.py:118-121`). `rg -n AVAILABLE_ACTIONS src tests` returns only the definition (Appendix C). The live action mapping used by the pipeline is `ActionType` itself — `dict_service.py:84` (`[ActionType.DELETE, ActionType.REPLACE, ActionType.ADD]`), `dict_service.py:90-92` (switch), and `db_service.py:638-642` (switch). `DBWorker` imports and uses `ActionType` (line 12) for its own switch, making `AVAILABLE_ACTIONS` a second, unread representation of the very rule the same class already follows via `ActionType`.
**Repro:** `inspect.getsource(DBWorker).count("AVAILABLE_ACTIONS")` → `1` (definition only); `rg AVAILABLE_ACTIONS src tests` → one hit (Appendix C).
**Impact:** Two homes for the action→code map: a future action code change must be made in `ActionType` (the live rule) and risk being missed in the dead `AVAILABLE_ACTIONS`, which then silently lies to any reader who trusts it.
**Root cause:** A value table was inlined as a class attribute instead of reusing the `ActionType` vocabulary; it was never wired to a reader.
**Fix:** Delete `DBWorker.AVAILABLE_ACTIONS` (the live rule already lives in `ActionType`); if a dict form is genuinely needed, build it from the enum (`{a.name: a.value for a in ActionType}`). Target: `db_service.py:64-68`.
**Verify:** new test needed: `test_no_dead_duplicate_of_action_type` (assert `AVAILABLE_ACTIONS` is absent and `ActionType` is the sole action mapping). No shipped test references `AVAILABLE_ACTIONS`.
**Related:** QLT-001 (same "two homes" theme), Block 2, Block 6.

#### QLT-004 — [LOW] Russian text in non-user-facing internal comments and module docstring

**Anchor:** `core/db_service.py::"восстановим после clean_names"` (secondary `core/db_service.py::"обычные колонки → TEXT по умолчанию"`, `:213`, `:717`, `:725`; `app.py::"основной публичный интерфейс"`)
**Disambiguator:** `восстановим`
**Checked:** ran (grep for Cyrillic in `src/`)
**Why this band:** taxonomy LOW = "documentation, reachability, and hygiene drift with no runtime consequence today"; project rule 1 ("All comments, logs, docstrings… must be in clear English") is a cheap-high-volume convention (Block 7). CRITICAL is not used — no runtime/logic consequence.
**Problem:** Six non-user-facing sites carry Russian: five inline comments in `core/db_service.py` (187, 209, 213, 717, 725) and the `app.py` module docstring (line 2). User-facing CLI text in `app.py` (`console.print`, `typer.Option(help=…)`) and the `run_report` Russian banners are out of phase scope (phase 16 / already filed ENT-003) and are excluded here.
**Repro:** `rg -n "[А-Яа-яЁё]" src/mko_data_cleaner/core/db_service.py` → 5 matches (lines 187, 209, 213, 717, 725); `rg` on `app.py` line 2 (module docstring) (Appendix E).
**Impact:** A reader grepping for the English term "restore" (or whatever `clean_names` recovery means) misses the comment; the English-only invariant is not enforced, so non-English drifts back in unnoticed on edit.
**Root cause:** Comments were written in the author's native language; no lint/automation enforces ASCII/English on comments.
**Fix:** Translate the six sites to English; optionally add a non-ASCII-in-comments gate to CI. Targets: `db_service.py:187/209/213/717/725`, `app.py:2`.
**Verify:** new test needed: `test_core_comments_english_only` (scan `core/*.py` comments + module docstrings for non-ASCII and assert zero). No shipped test enforces comment language.
**Related:** ENT-003 (phase 01, the `print()` Russian banners — different surface: this is comments/docstring, not console output), Block 7.

#### QLT-005 — [LOW] Commented-out dead `print()` debug calls left in production modules

**Anchor:** `core/db_service.py::"# print(query)"`, `core/dict_service.py::"# print(df_result[MappingColumns.pattern].to_list())"`
**Disambiguator:** `# print(`
**Checked:** static (grep)
**Why this band:** taxonomy LOW = hygiene/drift; these are disabled (commented) so they execute no I/O, but they are unreferenced dead code that violates the project's "no `print()`" spirit and the phase Block 7 sweep. ENT-003/PIPE-008 already cover *live* `print()`; this covers the commented remains.
**Problem:** `db_service.py:414` (`# print(query)`) and `dict_service.py:261` (`# print(df_result[MappingColumns.pattern].to_list())`) are commented-out debug prints left in the shipped `core/` modules. They are reachable code positions (the enclosing functions run on every report) that do nothing but confuse maintainers into thinking the logging is toggled there.
**Repro:** `rg -n "# print\(" src/mko_data_cleaner/core` → exactly two matches (Appendix F).
**Impact:** Readers mistake the commented line for an active (perhaps debug-gated) output; the two surviving live `print()` sites in `core/` (ENT-003/PIPE-008) remain the only enforced convention, so the commented copies suggest an inconsistent discipline.
**Root cause:** Debug `print`s were commented out instead of removed when promoted to `logger.debug`.
**Fix:** Delete the two commented lines (`db_service.py:414`, `dict_service.py:261`). Target: both lines.
**Verify:** new test needed: `test_no_commented_print_in_core` (grep `core/` for `# print(` and assert zero). No shipped test scans for commented `print`.
**Related:** ENT-003, PIPE-008, Block 7.

#### QLT-006 — [LOW] `is_valid_name`/`clean_names` docstrings contradict their behavior

**Anchor:** `core/utils.py::is_valid_name`, `core/utils.py::clean_names`
**Disambiguator:** `list of names to be checked in string format`
**Checked:** ran
**Why this band:** taxonomy LOW = "documentation, reachability, and hygiene drift… a documented rule the code contradicts misleads every later reader" (Block 8: a comment describing a condition differently from the executing path). No runtime consequence — the code's actual behavior is reasonable; only the prose is wrong.
**Problem:** `is_valid_name` (`utils.py:44-63`) documents `:param name: str, list of names to be checked in string format` and "Check whether provided name or list of names are valid", but its signature is `name: str` and line 52 raises `ValueError` when `name` is not a `str` — so lists are rejected, the documented case. `clean_names` (`utils.py:81-101`) carries a copy-pasted docstring that still says "Check whether provided name or list of names are valid" (it *checks*), but the function *cleans* names (`clean_names('a b') -> ['ab']`), not checks them.
**Repro:** `is_valid_name(["a","b"])` raises `ValueError: Invalid name: ['a','b'], should be a string, <class 'list'> is given` despite the docstring promising list support; `clean_names('a b')` returns `['ab']` (it cleans, contradicting "Check…are valid") (Appendix G).
**Impact:** A caller trusting the docstring passes a list to `is_valid_name` and gets `ValueError`; a maintainer reading `clean_names`'s docstring expects a boolean check and is surprised by a normalized name list.
**Root cause:** Docstrings were copy-pasted from an earlier list-accepting signature and never updated when the functions were narrowed/differentiated.
**Fix:** Correct `is_valid_name`'s docstring to state it accepts a single `str` and raises on non-str; correct `clean_names`'s docstring to describe normalization (deduplication + `make_valid`), not checking. Targets: `utils.py:46-50` and `utils.py:82-87`.
**Verify:** new test needed: `test_is_valid_name_rejects_non_str_with_documented_error`; no shipped test asserts list-passing or the "check" framing.
**Related:** Block 7, Block 8.

#### QLT-007 — [LOW] basedpyright framework-stub decline (`reportMissingTypeStubs="none"`) has no recorded rationale

**Anchor:** `pyproject.toml::"[tool.basedpyright]"`
**Disambiguator:** `reportMissingTypeStubs = "none"`
**Checked:** static (config file + grep for suppressions)
**Why this band:** taxonomy LOW = config-auditability hygiene with no runtime consequence. Phase Block 4 requires "the reason the project recorded for each" deliberately-declined diagnostic; here none is recorded. The suppression population is otherwise minimal and clean, so this is a single missing-rationale gap, not a noise explosion.
**Problem:** `pyproject.toml:152-154` configures `[tool.basedpyright]` with `typeCheckingMode = "standard"` and `reportMissingTypeStubs = "none"` and nothing else. `reportMissingTypeStubs` is the one suppressed diagnostic class (it silences "missing stubs" for third-party libs like polars/adbc/json5/unidecode). No adjacent comment or doc records *why* it was declined. The in-code suppression surface is one `# noqa: F401` at `db_service.py:50` (`import pyarrow` — import-for-side-effect, still warranted per the `_ensure_pyarrow_importable` docstring) and **zero** `# type: ignore` in `src/` (Appendix F). The 39 diagnostics `basedpyright` emits are all live `standard`-mode errors in `core/` (phase 04 owns their details); none are suppressed.
**Repro:** `rg -n "type: ignore|pyright:" src/` → no matches; `rg -n "noqa" src/` → one match (`db_service.py:50`); `git ls-files pyproject.toml` shows the `[tool.basedpyright]` block with no rationale comment (Appendix F, Appendix H).
**Impact:** A future maintainer cannot tell whether `reportMissingTypeStubs = "none"` was an intentional "these libs lack py.typed, suppress the noise" choice or an accidental default, risking either silent type-safety loss or a needless re-enable that re-floods output.
**Root cause:** The only non-default checker setting was added without an accompanying rationale.
**Fix:** Add a one-line rationale comment to the `[tool.basedpyright]` block (e.g. `# reportMissingTypeStubs=none: polars/adbc/json5/unidecode ship no py.typed stubs`). Target: `pyproject.toml:152-154`.
**Verify:** new test needed: `test_basedpyright_config_records_rationale` (parse `[tool.basedpyright]` and assert any non-default `report*` setting has an accompanying comment). No shipped test inspects the checker config.
**Related:** Block 4, Block 7; type-error clusters are phase 04, not re-filed here.

## Fix Order

- **Dependencies:** None. All seven touch disjoint symbols; QLT-001 and QLT-003 both touch the identifier/action vocabularies but at different modules (`utils`/`models` vs `db_service`).
- **Merge candidates:** QLT-001 + QLT-002 are both "constant-surface discipline in `models.py`+" (`NameConstrained` regex, `DictColumnsIndexes` numbering) — one coordinating pass. QLT-004 + QLT-005 + QLT-006 are all the Block 7 language/dead-code/docstring sweep in `core/` + `app.py` — one PR.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** QLT-003's `AVAILABLE_ACTIONS` overlaps the phase-01/04 `print()`/dead-code notes in spirit only; it is a *value-table* duplicate, not a print site. QLT-004's CLI-facing Russian is explicitly deferred to phase 16 / ENT-003 (only internal comments + module docstring are in scope here). QLT-007 deliberately does NOT re-file the 39 `basedpyright` errors (phase 04 owns those).
- **Rollout risk:**
  - QLT-001: extracting `IDENTIFIER_PATTERN` and pointing `NameConstrained` at it is behavior-preserving if the strings are equivalent (runtime repro confirms they agree); keep the test gate.
  - QLT-003: deleting an unread class attribute — `rg` proves zero readers; safe.
  - QLT-002: changing the numbering derivation changes `_initialize_mapping` slicing — gate on `test_dict_service.py` (`dict_indexes`/`sample_dictionary`) staying green.
  - QLT-004/005/006: doc/comment/code deletions; `uv run pytest` (93) the guard.
  - QLT-007: adding a comment to `pyproject.toml`; no behavioral change.

## Appendices

### Appendix A — Tooling baseline + cited-path gate

```text
$ git ls-files \
  src/mko_data_cleaner/core/utils.py \
  src/mko_data_cleaner/core/models.py \
  src/mko_data_cleaner/core/db_service.py \
  src/mko_data_cleaner/core/dict_service.py \
  src/mko_data_cleaner/app.py pyproject.toml
pyproject.toml
src/mko_data_cleaner/app.py
src/mko_data_cleaner/core/db_service.py
src/mko_data_cleaner/core/dict_service.py
src/mko_data_cleaner/core/models.py
src/mko_data_cleaner/core/utils.py

$ uv run ruff check src/ tests/
All checks passed!

$ uv run pytest -q
93 passed in 3.94s

$ uv run basedpyright src/  (count)
39 errors, 0 warnings, 0 notes
```

### Appendix B — Evidence: QLT-001 identifier regex duplication

```text
$ rg -n 'ALLOWED_PATTERN|NameConstrained' src/mko_data_cleaner/core/utils.py src/mko_data_cleaner/core/models.py
core/utils.py:15: ALLOWED_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$"
core/utils.py:44: def is_valid_name(name: str, pattern: str = ALLOWED_PATTERN) -> bool:
core/models.py:30: NameConstrained = Annotated[
core/models.py:33:         pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
core/models.py:42:     table_name: NameConstrained
core/models.py:43:     column_name: NameConstrained
```

Runtime repro (`uv run python .ai/tmp/repro_phase10.py`):
```text
== Block 1/6: identifier-validation regex in two homes
  utils.ALLOWED_PATTERN   = '^[a-zA-Z_][a-zA-Z0-9_]*$'
  models.NameConstrained pattern = ^[A-Za-z_][A-Za-z0-9_]*$
  behavior agree on ['Ab_id', '_a1', '1bad', 'a-b', 'a b'] = True
```
*(supports: two semantic-equal but textually distinct literals with no shared symbol.)*

### Appendix C — Evidence: QLT-003 AVAILABLE_ACTIONS has zero readers

```text
$ rg -n "AVAILABLE_ACTIONS" src tests
src/mko_data_cleaner/core/db_service.py:64:    AVAILABLE_ACTIONS = {

Runtime: inspect.getsource(DBWorker).count("AVAILABLE_ACTIONS") = 1  (definition only)

$ rg -n "ActionType" src
core/models.py:118: class ActionType(StrEnum):
core/dict_service.py:84,90,92 ... (live usage)
core/db_service.py:12 (import), 638,640,642 (case)  (live usage)
```

### Appendix D — Evidence: QLT-002 col-index gap + silent drop

Runtime repro (`uv run python .ai/tmp/repro_phase10.py`):
```text
== Block 2: DictColumnsIndexes numbering gap
  defaults = {'action': 0, 'match': 1, 'search': 2, 'term': 4}
  sorted values = [0, 1, 2, 4] | gap at index = [3]
== Block 2: silent-drop consequence in _initialize_mapping
  action_idx (sorted by value) skips [3]
  code uses max(action_idx)+1 as mapping start -> dropped column position(s): [3]
```

Code path (`dict_service.py:96-114`): `action_idx = list(self.action_col_indexes.values())` → `[0,1,2,4]`; `data_start_idx = max(action_idx) + 1` → `5`; `_data_actions = self.data[:, action_idx]` selects 0,1,2,4; `_data_mapping = self.data[:, range(5, width)]`. Index 3 is in neither. The shipped `sample_dictionary` fixture (conftest.py:50-63) places `None` at index 3, masking the drop.

### Appendix E — Evidence: QLT-004 non-English internal sites

```text
$ rg -n "[А-Яа-яЁё]" src/mko_data_cleaner/core/db_service.py
  187:        # восстановим после clean_names
  209:        # обычные колонки → TEXT по умолчанию
  213:        # колонки с типами
  717:        # получаем реально используемые колонки
  725:        # обновляем только эти колонки

$ rg -n "[А-Яа-яЁё]" src/mko_data_cleaner/app.py
  2:    mko_data_cleaner — основной публичный интерфейс   (module docstring — in scope)
  [lines 29,37,49,51,58,71,73,88 are CLI help/console output — user-facing, deferred to phase 16]
```

### Appendix F — Evidence: QLT-005 commented prints + QLT-007 suppression surface

```text
$ rg -n "# print\(" src/mko_data_cleaner/core
core/db_service.py:414:            # print(query)
core/dict_service.py:261:        # print(df_result[MappingColumns.pattern].to_list())

$ rg -n "noqa|type: ignore|pyright:" src/
core/db_service.py:50:        import pyarrow  # noqa: F401

(in-code `# type: ignore` suppressions: 0)
```

### Appendix G — Evidence: QLT-006 docstring/behavior mismatch

Runtime repro (`uv run python .ai/tmp/repro_phase10.py`):
```text
== Block 7: is_valid_name docstring over-claims list support
  docstring param says: 'str, list of names to be checked in string format'
  signature is: name: str
  is_valid_name(list) -> ValueError: Invalid name: ['a', 'b'], should be a string, <class 'list'> is given
== clean_names docstring mislabels behavior
  docstring says: 'Check whether provided name ... are valid'
  clean_names('a b') -> ['ab'] (it cleans, not checks)
```

### Appendix H — Evidence: QLT-007 basedpyright config

```text
[tool.basedpyright]
typeCheckingMode = "standard"
reportMissingTypeStubs = "none"
```
(`pyproject.toml:152-154` — `reportMissingTypeStubs` declined to `"none"`, no rationale comment; this is the only non-default diagnostic setting. `typeCheckingMode` is `standard` and recorded per the project commands.)
