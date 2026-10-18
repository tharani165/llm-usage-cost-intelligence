# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze ingestion
# MAGIC Appends one batch (widget `batch_id`) from the landing table to `bronze_usage_events` **as received**
# MAGIC plus lineage columns (`_record_hash`, `_source`, `_bronze_loaded_at`). Nothing is cleaned or dropped here.
# MAGIC Re-running the same batch inserts 0 rows (idempotent). Also loads the pricing CSV into `bronze_model_pricing`
# MAGIC (**overwritten each run**; it is a tiny config table).

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from llm_cost_intelligence.incremental import rows_not_yet_loaded
from llm_cost_intelligence.pricing import load_pricing_csv
from llm_cost_intelligence.schemas import RAW_COLUMNS, RAW_EVENT_SCHEMA, RAW_PRICING_SCHEMA
from llm_cost_intelligence.transformations import add_bronze_metadata

incoming_raw = spark.table(CFG.table("landing")).filter(f"batch_id = '{BATCH_ID}'")
incoming = add_bronze_metadata(incoming_raw, "synthetic_generator")
bronze_exists = spark.catalog.tableExists(CFG.table("bronze_events").replace("`", ""))
existing = spark.table(CFG.table("bronze_events")) if bronze_exists else None

to_insert = rows_not_yet_loaded(existing, incoming)
inserted = to_insert.count()
to_insert.write.mode("append").saveAsTable(CFG.table("bronze_events"))
print(f"batch {BATCH_ID}: {incoming.count()} incoming, {inserted} inserted")

# COMMAND ----------

pricing_rows = load_pricing_csv(PRICING_CSV)
(spark.createDataFrame([tuple(r[c] or None for c in RAW_PRICING_SCHEMA.fieldNames()) for r in pricing_rows], RAW_PRICING_SCHEMA)
 .write.mode("overwrite").saveAsTable(CFG.table("bronze_pricing")))

# COMMAND ----------

# MAGIC %md ### Verify

# COMMAND ----------

bronze = spark.table(CFG.table("bronze_events"))
batch_count = bronze.filter(f"batch_id = '{BATCH_ID}'").count()
source_count = spark.table(CFG.table("landing")).filter(f"batch_id = '{BATCH_ID}'").count()
assert batch_count == source_count, f"Bronze has {batch_count} rows for {BATCH_ID}, landing has {source_count}"
missing = [c for c in RAW_COLUMNS + ["_record_hash", "_source", "_bronze_loaded_at"] if c not in bronze.columns]
assert not missing, f"Bronze is missing columns: {missing}"
assert dict(bronze.dtypes)["request_timestamp"] == "string", "raw timestamps must stay strings in Bronze"
print(f"OK: {batch_count} rows for {BATCH_ID}; total Bronze rows = {bronze.count()}")
display(bronze.groupBy("batch_id").count())
