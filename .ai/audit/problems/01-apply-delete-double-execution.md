# Problem 01 — `DBWorker._apply_delete` executes the identical DELETE statement twice

- **Status:** Open (documented 2026-10-15; not fixed — this investigation is
  documentation-only)
- **Severity:** Low (correctness-neutral today), cosmetic/maintainability defect
  with a small runtime cost
- **Component:** `src/mko_data_cleaner/core/db_service.py`, class `DBWorker`,
  method `_apply_delete`
- **Related:** discrepancy D-10 in
  [`docs/00-overview/open-questions.md`](../../../docs/00-overview/open-questions.md)

## Summary

`_apply_delete` builds one `DELETE` statement and executes it **twice in a
row**, each time through `perform_query` (which executes and commits). The
second execution can never match any rows, because the first execution already
removed them. The duplicated call is dead work and misleads readers into
thinking a second, different deletion pass is intended.

## Location and Evidence

`src/mko_data_cleaner/core/db_service.py`, `_apply_delete` (lines 630–643 at
the time of writing):

```python
def _apply_delete(self) -> None:

    sql = f"""
        DELETE FROM {self.target_table}
        WHERE rowid IN (
            SELECT DISTINCT data_rowid
            FROM {self._joined_matches_table}
            )
        """

    self.perform_query(sql)          # first execution + commit

    logger.debug(f"Apply delete rules to {self.target_table}")
    self.perform_query(sql)          # second execution + commit (no-op)
```

`perform_query` (`db_service.py`, lines 195–206) executes the statement on a
new cursor and calls `self.db_con.commit()`, so the DELETE is applied and
committed twice, in two separate transactions.

## Observed behavior

- **Correctness:** the defect is currently invisible in results. After the
  first execution the matched rows no longer exist, so the second execution's
  `rowid IN (subquery)` matches zero rows. DELETE is idempotent here, so
  outputs are unaffected (verified indirectly by end-to-end runs of the
  documentation investigation: deleted rows disappear exactly once).
- **Cost:** the second execution still **re-evaluates the subquery**
  (`SELECT DISTINCT data_rowid FROM <joined_matches_table>`) and performs a
  rowid lookup for every match — all of which miss. On reports with many
  matched rows this is pure waste, once per DELETE rule block (the method is
  called once per generated DELETE block from
  `generate_rules_blocks`).
- **Risk:** the code is only safe because nothing re-inserts rows into the
  target table between the two executions. If future work ever made the
  matches non-idempotent (for example a change that re-populates
  `full_matches_table`/`joined_matches_table` mid-pass, or a trigger that
  re-creates rows), the second DELETE would silently delete rows the first
  pass intentionally kept.

## Reproduction

1. Run the tool on any report with at least one DELETE rule
   (`action = d` in the dictionary), with `index_column` configured so
   `target_table` is the `data_table_distinct` view.
2. Enable `logger.debug` (or attach an sqlite3 trace) and observe two
   identical `DELETE FROM ... WHERE rowid IN (...)` statements per DELETE
   block.
3. Compare `SELECT changes()` after each execution: the first returns the
   number of deleted rows, the second returns `0`.

## Options

| Option | Change | Assessment |
|---|---|---|
| A (recommended) | Delete the second `self.perform_query(sql)` call; keep the `logger.debug` line | Minimal, behavior-preserving; removes the wasted pass |
| B | Keep one execution, drop the debug log and add a short comment that the operation is idempotent | Equally valid; less debuggability |
| C | Replace with a single statement guarded by `SELECT changes()` / rowcount check | Over-engineered for the current use case |

No option changes observable behavior; A is the smallest correct fix.

## Out of scope for this document

- The separate, still-unresolved behavior gap where ADD overwrites a value
  previously written by REPLACE in the same extra column (owner-confirmed
  intent: REPLACE then ADD-merge; current implementation produces only the
  ADD tags). See D-2 in
  [`docs/00-overview/open-questions.md`](../../../docs/00-overview/open-questions.md).
- The defensive INSERT branch of `_sync_tables` that cannot fire in the
  standard flow (documented in
  [`architecture.md`](../../../docs/00-overview/architecture.md)).
