# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup (run automatically by the other notebooks via `%run ./00_setup`)
# MAGIC Defines the widgets, makes `src/` importable, builds the shared `CFG`, and creates the schema.
# MAGIC
# MAGIC **Requirement:** import this repo as a Databricks **Git folder** (Workspace → Create → Git folder) so
# MAGIC `src/` and `config/` sit next to `notebooks/`. All data here is synthetic; prices are fictional.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog (must allow CREATE SCHEMA, or use an existing schema)")
dbutils.widgets.text("schema", "llm_usage", "Schema")
dbutils.widgets.text("n_requests", "20000", "Batch-1 requests (1..50000; use 100 for a quick run)")
dbutils.widgets.text("seed", "42", "Random seed")
dbutils.widgets.text("batch_id", "batch_001", "Batch to ingest (02_bronze_ingestion)")

# notebooks/ is the notebook's folder; the repo root is one level up.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(_notebook_path))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))
PRICING_CSV = os.path.join(REPO_ROOT, "config", "model_pricing_example.csv")

from llm_cost_intelligence.config import ProjectConfig  # noqa: E402

CFG = ProjectConfig(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    n_requests=int(dbutils.widgets.get("n_requests")),
    seed=int(dbutils.widgets.get("seed")),
)
BATCH_ID = dbutils.widgets.get("batch_id")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CFG.catalog}`.`{CFG.schema}`")
spark.sql(f"USE CATALOG `{CFG.catalog}`")
spark.sql(f"USE SCHEMA `{CFG.schema}`")
print(f"Spark {spark.version} | target: {CFG.catalog}.{CFG.schema} | repo: {REPO_ROOT}")
