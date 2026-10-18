# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic data
# MAGIC Writes two delivery batches (`batch_001` initial, `batch_002` incremental) into the **landing** table,
# MAGIC which stands in for the upstream source system. **Overwrites `landing_usage_events` only** (deterministic, safe to rerun).
# MAGIC All values are synthetic; providers and models are fictional.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from llm_cost_intelligence import data_generator as dg
from llm_cost_intelligence.schemas import RAW_EVENT_SCHEMA

rows1, expected1 = dg.generate_initial_batch(dg.GeneratorConfig(CFG.n_requests, CFG.seed, "batch_001"))
rows2, expected2 = dg.generate_incremental_batch(rows1)
print("Expected batch_001:", expected1)
print("Expected batch_002:", {k: v for k, v in expected2.items() if k != "corrected_request_ids"})

# COMMAND ----------

landing = spark.createDataFrame(dg.to_tuples(rows1 + rows2), RAW_EVENT_SCHEMA)
landing.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.table("landing"))

# COMMAND ----------

# MAGIC %md ### Verify: landing table vs. documented expectations

# COMMAND ----------

landing_table = spark.table(CFG.table("landing"))
landing_table.printSchema()
counts = {r["batch_id"]: r["count"] for r in landing_table.groupBy("batch_id").count().collect()}
print("rows per batch:", counts)
assert counts["batch_001"] == expected1["raw_rows"], "batch_001 row count differs from generator expectation"
assert counts["batch_002"] == expected2["raw_rows"], "batch_002 row count differs from generator expectation"
display(landing_table.limit(10))
