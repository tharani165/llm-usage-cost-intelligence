# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: validation, deduplication, quarantine
# MAGIC Rebuilds `silver_usage_events` and `quarantined_usage_events` from **all** Bronze rows, so late and
# MAGIC corrected records are folded in deterministically. **Both tables are overwritten each run**; Bronze is the
# MAGIC system of record, so nothing is lost. Rules: `docs/data_quality_rules.md`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F
from llm_cost_intelligence.transformations import build_silver

bronze = spark.table(CFG.table("bronze_events"))
result = build_silver(bronze, CFG)
result.events.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.table("silver_events"))
result.quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.table("quarantine"))

# COMMAND ----------

# MAGIC %md ### Verify: every Bronze row is in exactly one of Silver / quarantine

# COMMAND ----------

silver = spark.table(CFG.table("silver_events"))
quarantine = spark.table(CFG.table("quarantine"))
b, s, q = bronze.count(), silver.count(), quarantine.count()
print(f"bronze={b}  silver={s}  quarantine={q}")
assert b == s + q, "Bronze != Silver + Quarantine"
assert silver.select("request_id").distinct().count() == s, "duplicate request_id left in Silver"
display(quarantine.groupBy("quarantine_category").count())
display(quarantine.filter("quarantine_category = 'validation_failure'")
        .select(F.explode("rejection_reasons").alias("reason")).groupBy("reason").count().orderBy(F.desc("count")))
display(silver.select(F.explode("warnings").alias("soft_warning")).groupBy("soft_warning").count())
