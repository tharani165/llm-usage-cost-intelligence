"""End-to-end local run (no Delta, in-memory) of Bronze -> Silver -> pricing -> Gold with checks.

Usage:  python scripts/validate_project.py [--out evidence/local_validation_results.txt]
Needs Java 17+ for Spark 4. Prints and (optionally) saves a plain-text report.
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pyspark.sql import functions as F  # noqa: E402

from llm_cost_intelligence import data_generator as dg  # noqa: E402
from llm_cost_intelligence.config import ProjectConfig  # noqa: E402
from llm_cost_intelligence.incremental import rows_not_yet_loaded  # noqa: E402
from llm_cost_intelligence.local import local_spark  # noqa: E402
from llm_cost_intelligence.metrics import (  # noqa: E402
    budgets_dataframe, build_budget_utilization, build_model_performance,
    build_quality_metrics, build_usage_daily, run_reconciliation)
from llm_cost_intelligence.pricing import load_pricing_csv, price_events, pricing_dataframe  # noqa: E402
from llm_cost_intelligence.schemas import RAW_EVENT_SCHEMA  # noqa: E402
from llm_cost_intelligence.transformations import add_bronze_metadata, build_silver  # noqa: E402


def main(out_path: str | None) -> int:
    cfg = ProjectConfig()
    spark = local_spark()
    spark.sparkContext.setLogLevel("ERROR")
    lines: list[str] = []
    log = lambda s="": (print(s), lines.append(s))

    rows1, exp1 = dg.generate_initial_batch(dg.GeneratorConfig(cfg.n_requests, cfg.seed))
    rows2, exp2 = dg.generate_incremental_batch(rows1)
    pricing = pricing_dataframe(spark, load_pricing_csv(ROOT / "config" / "model_pricing_example.csv"))

    def load(rows, bronze):
        df = spark.createDataFrame(dg.to_tuples(rows), RAW_EVENT_SCHEMA)
        new = rows_not_yet_loaded(bronze, add_bronze_metadata(df, "synthetic_generator"))
        return new if bronze is None else bronze.unionByName(new)

    def run(bronze):
        bronze = bronze.cache()
        silver = build_silver(bronze, cfg)
        events, quarantine = silver.events.cache(), silver.quarantine.cache()
        priced = price_events(events, pricing).cache()
        usage, perf = build_usage_daily(priced), build_model_performance(priced)
        budget = build_budget_utilization(priced, budgets_dataframe(spark, cfg), cfg)
        checks = run_reconciliation(bronze, quarantine, events, priced, usage, perf, budget)
        return events, quarantine, priced, usage, perf, budget, checks

    ok = True
    bronze = load(rows1, None)
    for stage, expected_silver in (("BATCH 1", exp1["silver_rows"]), ):
        events, quarantine, priced, usage, perf, budget, checks = run(bronze)
        log(f"=== {stage}: generated rows={exp1['raw_rows']} expected silver={expected_silver}")
        log(f"bronze={bronze.count()} silver={events.count()} quarantine={quarantine.count()}")
        ok &= events.count() == expected_silver

    # Replay batch 1 (must insert nothing), then load batch 2.
    replay = load(rows1, bronze)
    log(f"replay of batch_001: bronze rows {bronze.count()} -> {replay.count()}")
    ok &= replay.count() == bronze.count()
    bronze = load(rows2, bronze)
    events, quarantine, priced, usage, perf, budget, checks = run(bronze)
    log(f"=== AFTER BATCH 2: bronze={bronze.count()} silver={events.count()} quarantine={quarantine.count()}")
    log("\n-- reconciliation --")
    for c in checks:
        log(f"[{'PASS' if c['passed'] else 'FAIL'}] {c['check']}: expected={c['expected']} actual={c['actual']}")
        ok &= c["passed"]
    log("\n-- quality metrics --")
    for r in build_quality_metrics(bronze, events, quarantine, priced).orderBy("metric_group", "metric_name").collect():
        log(f"{r['metric_group']:20s} {r['metric_name']:28s} {r['metric_value']}")
    log("\n-- model performance --")
    for r in perf.orderBy("provider", "model").collect():
        log(str(r.asDict()))
    log("\n-- budget utilization --")
    for r in budget.orderBy("application", "year_month").collect():
        log(str(r.asDict()))
    log("\n-- SQL files executed against the local tables --")
    gold_quality = build_quality_metrics(bronze, events, quarantine, priced)
    for name, df in {"gold_usage_daily": usage, "gold_model_performance": perf,
                     "gold_budget_utilization": budget, "gold_quality_metrics": gold_quality,
                     "silver_priced_usage_events": priced, "silver_usage_events": events,
                     "quarantined_usage_events": quarantine}.items():
        df.createOrReplaceTempView(name)
    for sql_file in sorted((ROOT / "sql").glob("*.sql")):
        text = "\n".join(l for l in sql_file.read_text(encoding="utf-8").splitlines()
                         if not l.strip().startswith("--"))
        for n, stmt in enumerate([t.strip() for t in text.split(";") if t.strip()], start=1):
            try:
                rows = spark.sql(stmt).collect()
                log(f"[PASS] {sql_file.name} statement {n}: {len(rows)} rows")
            except Exception as exc:  # report and keep going
                ok = False
                log(f"[FAIL] {sql_file.name} statement {n}: {str(exc).splitlines()[0][:200]}")
    log(f"\nOVERALL:{'PASS' if ok else 'FAIL'}")
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=None)
    sys.exit(main(parser.parse_args().out))
