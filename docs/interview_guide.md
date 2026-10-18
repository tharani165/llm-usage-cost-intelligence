# Interview guide (talking points, not claims of production use)

**One-liner.** A medallion lakehouse over *synthetic* LLM request logs: validate, deduplicate deterministically,
price with effective-dated *fictional* rates in exact decimals, and publish reconciled cost/quality analytics.

**Decisions worth explaining**
1. *Raw timestamps stay strings in Bronze.* A bad value must reach quarantine with a reason, not crash the load.
2. *Dedup is a total order* (`ingested_at`, `batch_id`, `_record_hash`), so the result never depends on partition
   or row order. A test permutes the input to prove it. Only valid rows compete, so a bad correction can't win.
3. *Quarantine holds both validation failures and superseded duplicates*, which makes
   `bronze = silver + quarantine` an exact, checkable identity.
4. *Unpriced ≠ free.* Missing price → `unpriced`, NULL cost, surfaced in coverage metrics. `rate_limited` is an
   explicit, documented `not_billed` zero.
5. *Gold is additive.* `gold_usage_daily` stores sums/counts only, so every ratio re-aggregates correctly in SQL;
   non-additive p95 lives in its own table.
6. *Reconciliation fails the run* (notebook 05 asserts), rather than printing numbers nobody checks.
7. *Idempotent bronze, full-refresh silver/gold.* Simple and correct at this scale. Trade-off: it won't scale to
   very large history; next step is Delta `MERGE` / incremental Silver.

**Honest limitations to volunteer.** Synthetic data; fictional prices; Free Edition only; dashboard built
manually; Delta `MERGE`, Auto Loader, streaming and Jobs orchestration not implemented.

**Likely questions**
- *Why not `dropDuplicates`?* No control over which duplicate survives, no audit trail, order-dependent.
- *Why DECIMAL?* Float sums drift; the reconciliation compares totals for exact equality.
- *How would you scale it?* Incremental Silver via `MERGE` on `request_id`, Auto Loader for landing, partition
  Gold by date, a Lakeflow Job with the notebook order and the reconciliation as a task gate.
