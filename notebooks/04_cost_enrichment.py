# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Pricing and cost enrichment
# MAGIC Types and validates the pricing config (`silver_model_pricing`), joins the price in force on each request's
# MAGIC date, and writes `silver_priced_usage_events`. **Overwrites both tables.** Prices are **fictional**.
# MAGIC Unmatched records are `unpriced` with NULL cost (never 0); `rate_limited` requests are `not_billed` (explicit 0).
# MAGIC Billing convention: `docs/cost_calculation.md`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from decimal import Decimal
from pyspark.sql import functions as F
from llm_cost_intelligence.pricing import price_events, pricing_dataframe, validate_pricing_rows
from llm_cost_intelligence.schemas import RAW_PRICING_SCHEMA

raw_pricing = [r.asDict() for r in spark.table(CFG.table("bronze_pricing")).collect()]
raw_pricing = [{k: (v or "") for k, v in r.items()} for r in raw_pricing]
problems = validate_pricing_rows(raw_pricing)
assert not problems, problems

pricing = pricing_dataframe(spark, raw_pricing)
pricing.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.table("silver_pricing"))

silver = spark.table(CFG.table("silver_events"))
priced = price_events(silver, spark.table(CFG.table("silver_pricing")))
priced.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.table("priced_events"))

# COMMAND ----------

# MAGIC %md ### Verify

# COMMAND ----------

priced = spark.table(CFG.table("priced_events"))
assert priced.count() == silver.count(), "pricing join changed the row count"

# Hand-calculated check: 1,000 input @ 3.00/M + 500 output @ 15.00/M = 0.003 + 0.0075 = 0.0105
probe = spark.createDataFrame([(1000, 500, Decimal("3.000000"), Decimal("15.000000"))],
                              "input_tokens long, output_tokens long, input_price_per_million_tokens decimal(12,6), "
                              "output_price_per_million_tokens decimal(12,6)")
from llm_cost_intelligence.pricing import calculate_costs
got = calculate_costs(probe.withColumn("status", F.lit("success"))).first()["total_estimated_cost"]
assert got == Decimal("0.01050000"), got
print("manual cost check OK:", got)

assert priced.filter("billing_status = 'unpriced' AND total_estimated_cost IS NOT NULL").count() == 0
display(priced.groupBy("billing_status").agg(F.count("*").alias("requests")))
display(priced.filter("billing_status = 'unpriced'").groupBy("provider", "model").count())
