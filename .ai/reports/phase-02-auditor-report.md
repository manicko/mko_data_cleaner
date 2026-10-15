---
phase: "02"
phase_name: "Configuration & Secrets Management"
phase_file: ".kilo/commands/audit/phases/02-audit-config-secrets.md"
report_status: "draft"
scope: >
  configuration declaration, resolution, validation, and dead/unconsumed
  configuration in a synchronous, single-shot, local-only CLI tool + library
  (no network, no credentials, no authentication).
---

# Audit Report — Phase 02 Configuration & Secrets Management

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **synchronous, single-shot, local-only** CLI tool and
Python library that post-processes Mediascope TV advertising exports received as
files on disk. There is **no network, no credentials, no authentication**
anywhere in the code, the dependency list, or the runtime dependency graph.

Configuration is delivered as **two YAML files** that ship as package data and
are copied to the user settings directory by an `init` step:

- **`app_config.yaml`** — validated as a single, flat `DataSettings` Pydantic v2
  model (`core/models.py:157-163`) with nested `WorkingPaths`, `DataFile`,
  `DataDict`, `Database`, `ReadCSV`/`WriteCSV` submodules. It is **not** a
  defaults-plus-overlay merge; there is no "defaults directory" and no
  "report-specific overlay." The merge helper `merge_dicts`
  (`core/utils.py:152-173`) exists but is **dead code** — a repo-wide grep
  finds zero call sites in `src/` or `tests/`.
- **`log_config.yaml`** — validated as `LoggingSettings`
  (`core/models.py:50-56`), consumed by `logging.config.dictConfig` inside the
  lazy factory (`app_service.py:302`).

Key runtime facts, code-verified against `src/mko_data_cleaner` unless noted:

- **Zero async.** A repo-wide grep for `asyncio`, `async def`, `asyncio.run`,
  `Semaphore`, and `Queue` across `src/mko_data_cleaner` returns **no
  matches** (`app.py`, `core/app_service.py`, `core/utils.py`). The CLI is plain
  Typer; the library API is synchronous (`process_data`). There is no
  concurrency layer at all.
- **No credentials.** A grep for `connections` in `src/` returns nothing; there
  is no `connections/` directory anywhere in the tree (glob confirmed).
  `product-overview.md` states the tool "never contacts Mediascope" and
  `architecture.md` lists "External systems: none." Exports arrive manually.
- **Template distribution.** Templates live at
  `src/mko_data_cleaner/settings/{app_config,log_config}.yaml`, shipped inside
  the wheel/sdist via `[tool.setuptools.package-data] mko_data_cleaner =
  ["settings/*.yaml"]` (`pyproject.toml:61`) and `MANIFEST.in:3`. The `init`
  command calls `init_project()` → `_copy_templates()`
  (`core/init_service.py:30-42`), which always writes to the **hardcoded**
  `APP_PATHS.user_settings_dir` regardless of the `settings_dir` argument
  passed to `initialize_settings` (`app.py:121-127`, NOTE comment).
- **Settings gate.** `get_app_service()` (`app_service.py:290-303`) runs
  `_require_settings()` (`app_service.py:272-287`) **before** constructing the
  service and applying `dictConfig`. The gate checks **existence only**: if both
  YAMLs exist, it passes — even when one is malformed. Malformed YAML passes the
  gate and fails later as `ConfigError` during `DataSettings` validation
  (`app_service.py:41-46`). `yaml_to_dict` (`core/utils.py:131-149`) returns `{}`
  on YAML error or missing file (defensive, tested at
  `tests/test_app.py:207-215`).
- **Entry surfaces.** Console script `mko-data-cleaner =
  "mko_data_cleaner.app:app"` (`pyproject.toml:46`); module entry
  `python -m mko_data_cleaner.app` (`app.py:130`, the only working ` -m `
  form — there is **no** `__main__.py`, so `python -m mko_data_cleaner` does
  not launch the CLI, confirmed by glob); library API
  `process_data(report_path, settings_dir=None)` and
  `initialize_settings(force, settings_dir=None)` (`app.py:105,121`).
- **Public API.** `app.py.__all__` (`app.py:134-142`) exports the Typer
  symbols (`app`, `init`, `run`, `list_settings`, `get_app_service`) alongside
  the two contract functions; the package root `__init__.py` exposes **only**
  `__version__` (no re-exports), so `from mko_data_cleaner import
  process_data` fails (D-8 #4).
- **Stale doc drift on log_config.** The documentation trio —
  `open-questions.md` D-8 #6, `current-behavior.md:314-317`, and the Phase 01
  auditor report (line 242) — all state that `log_config.yaml` contains stale
  `tests` and `telebot` logger entries. The **actual** file
  (`src/mko_data_cleaner/settings/log_config.yaml`) contains **only** the
  `__main__` logger (`log_config.yaml:44-51`); no `tests` or `telebot`
  entries exist. The code is already clean here; the documentation is stale.
- **Dead dependencies.** `pyproject.toml:17-32` declares `pandas`,
  `pydantic-settings`, `python-dateutil`, and `urllib3`. A grep for imports of
  each across `src/` returns **zero** matches; `pandas` survives only as a
  string in a code comment (`db_service.py:512`). These are unconsumed
  declarations — noise for any reader auditing the dependency/config surface.

---

## 2. Per-Block Assessment (1–7)

### Block 1 — Settings surface & template distribution topology

- **Fit:** **Medium–High.** The core concept is valid and partially present:
  package-shipped YAML templates, a user settings directory, and an `init` copy
  mechanism all exist. Asking which variant each surface loads, how user edits
  are preserved across re-init, and whether the copy is atomic are all
  legitimate questions for this tool.
- **Outdated:**
  - "the defaults directory" **does not exist.** There are only two YAML
    files: the *package* templates (`settings/`) and the *user* copy
    (`user_settings_dir`). There is no third "defaults" tier and no report-specific
    overlay. The block's tripartite "package / user config dir / defaults dir"
    model is a phantom.
  - "each running surface loads" — there is only one real config consumer
    (`AppService.app_config` / `AppService.log_config`, both `cached_property`
    on the service built by `get_app_service`). The CLI, library, and module
    entry all funnel through the same factory.
- **Gap:** The block **does not** ask the one real asymmetry that matters:
  `init`/`initialize_settings` **ignore** a caller-supplied `settings_dir`
  (`init_service.py:30-42` always writes to `APP_PATHS.user_settings_dir`;
  `app.py:121-127` admits this explicitly), while `run`/`process_data` honor it
  (`build_app_paths`, `app_service.py:258-269`). A user can `init` into the
    default location, then `run` with `--settings-dir X`, and never hit
    `SettingsNotInitializedError` against the wrong dir — or vice versa, `run`
    with `--settings-dir` then `init` (which writes to the default) leaves the
    run looking in the wrong place.
- **Overlap:** Overlaps Block 6 (entry-surface parity) and Block 3 (validation)
  mildly. Acceptable if Block 6 is retargeted.

### Block 2 — Credential seam: where the Mediascope secret enters

- **Fit:** **None.** There is no Mediascope API credential. There is no network
  call, no `connections/` directory, no JSON secret file, no header/path/body
  injection. A grep for `connections` in `src/` returns nothing; a glob for
  `**/connections/**` returns nothing.
- **Outdated:** **Entirely.** "The Mediascope API credential is a JSON file in
  `connections/`" (Phase 02 Purpose, line 19) is fiction. There is no API to
  fetch from, no credential lifecycle, no "holding surface," no "three states
  (absent/empty/malformed)." Even the "wire form" question is moot — nothing is
  sent over a wire.
- **Gap:** None to leave standing.
- **Recommendation:** **Delete Block 2 in its entirety.** It has no target in
  this codebase. (See §3 for the Purpose correction that removes the premise.)

### Block 3 — Pydantic validation fidelity & defaults merge

- **Fit:** **Partial — the Pydantic half only.** Pydantic v2 validation exists
  and is real: `DataSettings` (`models.py:157`) and `LoggingSettings`
  (`models.py:50`) are validated via `**config_data` from `yaml_to_dict`, and a
  `DataValidationError` is wrapped as `ConfigError`
  (`app_service.py:41-46`). The consumer (AppService) reads the **validated
  model**, not the raw dict.
- **Outdated:**
  - "the base defaults from the defaults template, overlaid with the
    report-specific values" — **no such merge chain exists.** `app_config.yaml`
    is loaded directly from the user settings directory via `yaml_to_dict` →
    `DataSettings(**config_data)`. There is no package-default dict overlaid
    with a user override; there is no report-specific overlay. `merge_dicts`
    (`utils.py:152-173`) is the nearest thing to a "defaults merge" primitive,
    but it is **dead code** (zero call sites) and operates on arbitrary dicts,
    not on config defaults.
  - "fails fast with a named field" for type mismatches — partially true (a
    malformed required key raises a `ValidationError` that names the field),
    but only **after** the existence-only gate passes. The block's framing
    assumes validation happens on a merged dict before an "API call"; here there
    is no API call, and validation is deferred to cached_property access.
- **Gap:** The **malformed-YAML passthrough** is the real validation-fidelity
  defect, and the block's "merge" framing obscures it: `_require_settings`
  tests existence only (`app_service.py:279-283`); a present-but-broken YAML
  passes the gate and only fails when `app_config`/`log_config` is first
  accessed, producing `ConfigError` from a wrapped `DataValidationError`
  (`app_service.py:45-46`). `yaml_to_dict` swallows the YAML syntax error and
  returns `{}`, masking *why* the file is bad — the user sees a Pydantic
  "field required" error, not "your YAML is syntactically invalid." This is a
  diagnosability gap the block should surface.
- **Overlap:** Overlaps Block 4 (fail-fast) and Block 5 (unconsumed config,
  via `merge_dicts`). The "defaults merge" concept overlaps no real structure.

### Block 4 — Secret validation & fail-fast behaviour

- **Fit:** **Partial — the fail-fast half only.** The four questions
  (guard present / unconditional / can a value disable it / actionable +
  secret-free failure) are a fine template for any config guard. The
  **fail-fast** concept maps to `_require_settings`: it is unconditional and
  raises `SettingsNotInitializedError` (a `FileNotFoundError` subclass,
  `errors.py:20-29`) naming the missing files and the remediation
  (`app_service.py:272-287, 290-303`).
- **Outdated:**
  - "the credential guard" — there is no credential guard. The four questions
    are framed around "the secret," the "three states (absent/empty/malformed)
    of a credential," and "the file they expect" — none of which apply.
  - "absent/empty/malformed" for a **missing settings file** is only partially
    mapped: missing → raises fast; but **malformed** is *not* caught at the
    guard (see Block 3 Gap). The block does not ask the right question of the
    real guard because it is tuned to secrets.
- **Gap:** The block misses the actual fail-fast gap: existence-only checking
  means malformed-but-present config is **not** fail-fast; it degrades to a
  confusing `ConfigError` downstream. The "guard inventory" ask is sound, but
  for settings files, not secrets.
- **Recommendation:** **Rewrite as "Settings gate: fail-fast fidelity."**
  Delete the secret framing. Ask: (a) is the gate unconditional, (b) does it
  catch malformed-but-present YAML or only absence, (c) does the failure name
  the remediation, (d) does `yaml_to_dict`'s `{}` fallback mask the *cause* of
  a broken file.

### Block 5 — Dead, ineffective & unconsumed configuration

- **Fit:** **High.** This is the best-aligned block in the Phase 02 file. Asking
  "is each setting still read," "does every declared value reach a consumer,"
  and "is every environment branch reachable" is exactly right for a config
  layer.
- **Outdated:** None of substance. The three questions survive intact.
- **Gap — findings this block should surface (evidence below):**
  - **`merge_dicts` is dead code.** Defined `core/utils.py:152-173`; a
    repo-wide grep (src + tests) finds **zero** callers. It is not documented
    as needed by any overview doc. It is a candidate for investigation, not a
    live config consumer.
  - **Dead dependencies** (see §1): `pandas`, `pydantic-settings`,
    `python-dateutil`, `urllib3` are declared in `pyproject.toml:17-32` but
    have **zero** import sites in `src/`. `pandas` survives only as a
    comment string in `db_service.py:512`. These are unconsumed declarations on
    the config/dependency surface.
  - **`pydantic-settings`** is particularly relevant: the architecture claims it
    is "unused residue" (`architecture.md:179`); it is declared but never
    imported — no settings are sourced from environment variables through it.
    Block 5 should verify the claim and flag the dead dep.
  - **`log_config.yaml` logger drift is already resolved in code but not in
    docs** (see §1): the docs say `tests`/`telebot` loggers are present; the
    actual file has only `__main__`. The block should record this as a
    doc-code divergence, not a live dead-logger finding.
  - The stale dev `__main__` block at `app_service.py:306-307`
    (`get_app_service().run_report(r"data\mvideo")`) — unreachable from normal
    entry, hardcodes a data path, and embeds domain execution in an internal
    orchestrator module (also flagged by Phase 01 Block 5).
- **Overlap:** Mild overlap with Block 3 (via `merge_dicts`) and Block 4
  (malformed-YAML passthrough). Acceptable — this block is the natural home for
  dead-code/unconsumed findings.

### Block 6 — Environment separation & process parity

- **Fit:** **Partial.** The three surfaces exist and parity is a real concern:
  CLI (`run`), library (`process_data`), and module entry
  (`python -m mko_data_cleaner.app`). The `settings_dir` override is
  **inconsistent** between `run`/`process_data` (honored via `build_app_paths`)
  and `initialize_settings`/`init` (ignored) — this is precisely the
  "a value written into one surface's config can leak into another" risk the
  block asks about.
- **Outdated:**
  - "credential exposure" and "debug-level logging" as parity risks are
    inapplicable (no credentials; debug logging is purely a YAML setting).
  - "config-variant selection consistent across all three entry surfaces" —
    there is only **one** variant per setting file (not dev/prod/test YAML
    variants), so the question needs re-grounding.
- **Gap:** The block does not surface that `python -m mko_data_cleaner` (the
  bare package) is **not** a working entry — there is no `__main__.py`, so only
  `python -m mko_data_cleaner.app` works (`app.py:130` contains the
  `if __name__ == "__main__"` guard, confirmed by glob). The "process parity"
  question should include "does the module entry behave identically to the
  console script."
- **Overlap:** Overlaps Block 1 (template topology) and Block 3. Acceptable.

### Block 7 — Env-overridable production posture

- **Fit:** **Low.** Several of the cited posture-affecting settings simply do
  not exist in this tool: there is no concurrency limit (single-shot,
  synchronous), no retry count (no network, no transient failures), and no
  credentials file path.
- **Outdated:**
  - "the concurrency limit, the retry count, [and] the credentials file path"
    are all phantom settings for this codebase.
  - "production variant is supposed to harden it" — there is no production
    variant; a single config is copied verbatim from the template. No env-var
    override layer exists (and `pydantic-settings` — the natural mechanism — is
    a dead dependency, see Block 5).
  - "documented operator controls vs. drift" — the only operator control is
    `--settings-dir`; the rest are YAML fields.
- **Gap:** The block misses the two settings that *do* change operational
  posture and *are* config-driven (via `app_config.yaml`, not env vars):
  `data_paths.db_file` (the disposable SQLite DB lives inside the report dir by
  default — `architecture.md:233`, `data-flow.md:78`) and
  `read_settings.from_csv` / `export_settings.to_csv` (Polars reader/writer
  options, including compression and `has_header`). These affect output shape and
  location but have no env override and no production hardening.
- **Overlap:** Overlaps Block 1 (config sourcing) and Block 6 (parity of the
  `--settings-dir` control).

---

## 3. Phase Purpose / Scope Assessment

The **Purpose** (lines 13–19) and the inter-phase ownership note (lines 21–24)
describe a different project. Corrections required:

| Phase 02 text | Reality | Action |
|---|---|---|
| "standalone **async** CLI tool" | synchronous; no event loop, no `asyncio` (grep: 0 matches in `src/`) | remove "async" |
| "fetches data from the Mediascope API" | no network/credentials; exports arrive as files; `architecture.md:37-40` "External systems: none" | remove Mediascope API fetch |
| "The Mediascope API credential is a JSON file in `connections/`" | no credentials, no `connections/` dir (glob confirmed) | delete entirely |
| "the blocking-work bridge and retry contract (09)" | no blocking-work bridge; no retries | remove (09) exclusion reference |
| "the async pipeline (03, 04)" | no async pipeline | remove (03, 04) exclusion references |
| "credential provenance (02)" | no credentials at all | remove as inapplicable |

**Scope Boundaries** are vestigial: they exclude "credential provenance (02)"
as if it existed, and point to async/concurrency phases (03, 04, 09) that have
no subject here. The rewritten scope should (a) drop all async/concurrency and
credential references, (b) clarify that this phase owns **config sourcing,
validation fidelity, fail-fast gating, and dead/unconsumed configuration**
(not secrets), and (c) add the **`settings_dir` forwarding asymmetry** (init
vs. run) and the **malformed-YAML passthrough** as explicitly in scope — both
currently implied but not stated.

---

## 4. Severity Taxonomy Assessment

The **taxonomy's principle** — "rate what is true now, not the worst
consequence if triggered" and the CRITICAL/HIGH/MEDIUM/LOW banding notion — is
sound and should be **retained**. The **illustrative examples** are misaligned
because they are built around a credential-bearing, async runtime that does not
exist here.

| Band | Example in Phase 02 | Applies to current arch? | Reroute suggestion |
|---|---|---|---|
| CRITICAL | "credential obtainable by anyone who can read the file" | **No** — no credentials exist | Anchor to: a config gate that silently proceeds on missing/malformed config. Today `_require_settings` *does* raise, so this band is **empty** — a "do not regress" guard. |
| CRITICAL | "credential file empty/invalid/shared across environments" | **No** — no credential file | Drop. |
| CRITICAL | "config gate bypassed or silently disabled" | **Partially** — the existence-only gate is *not* bypassed for missing files, but it *is* a partial gate: malformed-but-present YAML passes it. Re-frame to "gate that proceeds past a malformed config" → today this is a real MEDIUM (confusing downstream `ConfigError`), not CRITICAL, because failure still occurs (just late and opaque). |
| HIGH | "secret values reaching logs/output/errors" | **No** — no secrets | Drop. |
| HIGH | "merged config value reaches consumer without validation → silent data corruption" | **Partially relevant** — config *is* validated (`DataSettings`), but the malformed-YAML/`{}` fallback path means a broken file produces an opaque Pydantic error rather than "file is invalid YAML." Re-frame to validation-opaqueness risk. |
| MEDIUM | "config-name drift; ineffective/unconsumed config; setting name validator no longer reads" | **Directly relevant** — `merge_dicts` dead code, dead deps (`pandas`, `pydantic-settings`, …), stale doc drift on `log_config.yaml` loggers. | Keep verbatim. |
| MEDIUM | "leakage of config across entry surfaces" | **Directly relevant** — `settings_dir` honored by `run`/`process_data` but ignored by `init`/`initialize_settings` (D-6). | Keep verbatim. |
| LOW | "placeholder/template gaps; undocumented switches; non-atomic copy" | **Relevant** — stale `__main__` dev block (`app_service.py:306-307`), non-atomic template copy (`_copy_templates` writes file-by-file, `init_service.py:15-27`), missing `__main__.py` so `python -m mko_data_cleaner` fails. | Keep, with the corrected examples. |

**Net:** keep the banding principle and the four severity labels; rewrite **every** example to drop the credential/secret/network/asyncio framing and anchor instead to (a) fail-fast-gate regression guard (CRITICAL, empty today), (b) malformed-config passthrough opaqueness (HIGH), (c) dead/unconsumed config and cross-surface `settings_dir` leakage (MEDIUM), (d) stale dev block / non-atomic copy / missing `__main__.py` / doc-code drift (LOW).

---

## 5. List of Concrete Problems to Address

Each item maps to a specific edit in
`.kilo/commands/audit/phases/02-audit-config-secrets.md`. **No source files
are modified by this report.**

1. **[PURPOSE] Strip the async/network/credential fiction.** Remove "async"
   from the purpose; replace the Mediascope-API-fetch framing with "post-processes
   Mediascope exports received as files on disk; no network, no credentials, no
   authentication."
   *Evidence: `docs/00-overview/architecture.md:37-40`; `product-overview.md:41-44`;
   grep for `asyncio`/`async def` returns 0 matches in `src/`.*

2. **[PURPOSE] Delete the credential premise.** Remove the sentence
   "The Mediascope API credential is a JSON file in `connections/`" — no
   `connections/` directory exists and no credential is loaded anywhere.
   *Evidence: glob `**/connections/**` returns nothing; grep for `connections`
   in `src/` returns nothing; `architecture.md` "External systems: none."*

3. **[PURPOSE/Scope] Remove the async/inter-phase exclusion references.**
   Drop the "(09) blocking-work bridge," "(03, 04) async pipeline," and
   "(02) credential provenance" exclusions — none are applicable (no async
   primitives, no blocking-work bridge, no credentials). Add an explicit
   statement that the **`settings_dir` forwarding asymmetry** (init vs. run)
   and the **malformed-YAML passthrough** (existence-only gate) are in scope.
   *Evidence: `app_service.py:272-287` (existence-only gate);
   `app_service.py:41-46` (validation deferred); `app.py:121-127` (NOTE:
   `settings_dir` not forwarded by `initialize_settings`).*

4. **[Block 2] Delete Block 2 — "Credential seam: where the Mediascope secret
   enters."** It has no target: no secret, no `connections/`, no network call,
   no header/path/body injection. *Evidence: §1 and grep/glob results above.*

5. **[Block 3] Remove the "defaults merge" framing.** Replace "the base defaults
   template overlaid with report-specific values" with: `app_config.yaml` is
   loaded directly via `yaml_to_dict` → `DataSettings(**config_data)`; there is
   no defaults directory, no overlay, and no report-specific config. Flag
   `merge_dicts` (`utils.py:152-173`) as **dead code** — zero call sites in
   `src/` or `tests/`.
   *Evidence: `app_service.py:41-46`; `utils.py:152-173`; grep for `merge_dicts`
   in src+tests finds only the definition.* Add the **malformed-YAML
   passthrough** as the real validation-fidelity defect: `yaml_to_dict`
   swallows syntax errors and returns `{}`, so a broken file surfaces as an
   opaque "field required" `ConfigError` rather than "your YAML is invalid."
   *Evidence: `utils.py:141-149`; `app_service.py:45-46`;
   `tests/test_app.py:207-215` (yaml_to_dict defense-in-depth test).*

6. **[Block 4] Rewrite as "Settings gate: fail-fast fidelity" (drop secrets).**
   Keep the four-question template for *the settings gate*, not a credential
   guard. Record: the gate (`_require_settings`, `app_service.py:272-287`) is
   unconditional and fail-fast on **missing** files, but it checks existence
   only and does **not** catch malformed-but-present YAML — that path degrades to
   `ConfigError` at cached_property access (`app_service.py:41-46`). The failure
   text names the missing file and `initialize_settings()` remediation
   (`errors.py:20-29`).
   *Evidence: `app_service.py:279-283, 290-303, 41-46`; `errors.py:20-29`.*

7. **[Block 5] Keep verbatim; enrich with verified dead-code findings.** This
   is the best-fitting block. Add: (a) `merge_dicts` dead code
   (`utils.py:152-173`, 0 call sites); (b) dead dependencies `pandas`,
   `pydantic-settings`, `python-dateutil`, `urllib3` declared in
   `pyproject.toml:17-32` with zero import sites in `src/` (pandas referenced
   only as a comment string in `db_service.py:512`); (c) the **doc-code
   divergence** on `log_config.yaml` — docs (`open-questions.md` D-8 #6,
   `current-behavior.md:314-317`, Phase 01 report line 242) claim stale
   `tests`/`telebot` loggers, but the actual file contains only `__main__`
   (`log_config.yaml:44-51`); (d) the stale dev `__main__` block
   `app_service.py:306-307`.
   *Evidence: all cited locations above; grep results.*

8. **[Block 6] Rewrite to drop credential exposure; keep surface parity.** Retarget
   the "config-variant selection / debug-level consistency / leakage" questions
   to the **three real entry surfaces** (CLI `run`, library `process_data`, module
   `python -m mko_data_cleaner.app`). Key check: `python -m mko_data_cleaner`
   (bare package) is **not** a working entry — no `__main__.py`
   (`app.py:130` holds the only `if __name__ == "__main__"` guard; glob confirms
   no `__main__.py`). Keep the cross-surface `settings_dir` leakage finding
   (D-6).
   *Evidence: `app.py:105-118, 130`; glob `**/__main__.py` in `src/` = empty;
   `app.py:121-127`; `app_service.py:258-269`.*

9. **[Block 7] Rewrite to drop concurrency/retry/credential posture settings.**
   Replace "concurrency limit, retry count, credentials file path" with the
   settings that *do* affect operational posture in this tool: the DB file
   location (`data_paths.db_file` — the disposable SQLite DB lives inside the
   report dir by default) and the Polars reader/writer options
   (`read_settings.from_csv`, `export_settings.to_csv`, incl. compression and
   `has_header`). Note none are env-overridable (no env layer; `pydantic-settings`
   is a dead dependency).
   *Evidence: `app_config.yaml:6, 28-49`; `architecture.md:233`;
   `data-flow.md:78`; `pyproject.toml:24` (pydantic-settings declared, 0 imports).*

10. **[Severity taxonomy] Re-base the examples.** Keep the banding principle and
    the CRITICAL/HIGH/MEDIUM/LOW labels. Rewrite examples to the real runtime:
    the CRITICAL band becomes a **fail-fast-gate regression guard** (empty
    today — the gate *does* raise on missing config; CRITICAL would be reverting
    to silent fallback); HIGH becomes **malformed-config opaqueness** (the
    `{}` fallback + deferred validation); MEDIUM stays **dead/unconsumed
    config + cross-surface `settings_dir` leakage**; LOW stays **stale dev
    block, non-atomic template copy, missing `__main__.py`, doc-code drift**
    on `log_config.yaml`. Remove all credential/secret/network/asyncio examples.
    *Evidence: see §4 table and cited locations above.*

11. **[Adjacent, Phase 02-relevant] Flag the over-exported `__all__` and empty
    package `__init__`.** `app.py:134-142` exports Typer internals
    (`app`, `init`, `run`, `list_settings`, `get_app_service`) alongside the two
    contract functions; `src/mko_data_cleaner/__init__.py` exposes only
    `__version__`, so `from mko_data_cleaner import process_data` fails
    (D-8 #4). This is a "settings/API surface" hygiene item relevant to any
    config-consumer audit.
    *Evidence: `app.py:134-142`; `__init__.py:1-10`.*

### Dependency / edit-ordering note

The Purpose/Scope and Severity fixes (items 1, 2, 3, 10) are **foundational**:
every block inherits the async/credential framing from the Purpose, and the
severity examples inherit it from the taxonomy. Correct these first — they
remove the largest class of errors at the source. Next: delete Block 2
(item 4) and rewrite Blocks 3, 4, 6, 7 away from secrets/async (items 5, 6, 8, 9).
Block 5 (item 7) and the `__all__` flag (item 11) are enrichment.

No findings from this report depend on changes to `docs/00-overview/*` or
`src/` — this audit examines the **audit phase file itself**; every `src/`
path cited was read from the working tree and verified (it is tracked in git),
and every `docs/00-overview/*` path was read from the working tree. Note that
`docs/00-overview/*` are currently **untracked** in git (they appear as
untracked under `docs/`), so those citations are working-tree evidence only,
consistent with how the Phase 01 auditor report cited the same docs. Grep and
glob results are stated explicitly where used as evidence.
