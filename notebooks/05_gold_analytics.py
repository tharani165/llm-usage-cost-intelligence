# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold analytics and reconciliation
# MAGIC Builds `gold_usage_daily`, `gold_model_performance`, `gold_budget_utilization`, `gold_quality_metrics`
# MAGIC (**overwritten each run**) and reconciles them with the eligible Silver records. The notebook fails if any
# MAGIC reconciliation check fails. Dashboard queries live in `sql/`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from llm_cost_intelligence.metrics import (
    budgets_dataframe, build_budget_utilization, build_model_performance,
    build_quality_metrics, build_usage_daily, run_reconciliation)

bronze = spark.table(CFG.table("bronze_events"))
silver = spark.table(CFG.table("silver_events"))
quarantine = spark.table(CFG.table("quarantine"))
priced = spark.table(CFG.table("priced_events"))

outputs = {
    "gold_usage_daily": build_usage_daily(priced),
    "gold_model_performance": build_model_performance(priced),
    "gold_budget_utilization": build_budget_utilization(priced, budgets_dataframe(spark, CFG), CFG),
    "gold_quality_metrics": build_quality_metrics(bronze, silver, quarantine, priced),
}
for key, df in outputs.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.table(key))

# COMMAND ----------

# MAGIC %md ### Reconciliation (Bronze → Silver/Quarantine → Priced → Gold)

# COMMAND ----------

checks = run_reconciliation(
    bronze, quarantine, silver, priced,
    spark.table(CFG.table("gold_usage_daily")), spark.table(CFG.table("gold_model_performance")),
    spark.table(CFG.table("gold_budget_utilization")))
for c in checks:
    print(f"[{'PASS' if c['passed'] else 'FAIL'}] {c['check']}: expected={c['expected']} actual={c['actual']}")
assert all(c["passed"] for c in checks), "Reconciliation failed; see FAIL lines above"

# COMMAND ----------

display(spark.table(CFG.table("gold_model_performance")))
display(spark.table(CFG.table("gold_budget_utilization")).orderBy("application", "year_month"))
display(spark.table(CFG.table("gold_quality_metrics")).orderBy("metric_group", "metric_name"))
