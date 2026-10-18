"""Local SparkSession for tests/validation scripts. Not used on Databricks (`spark` is provided)."""
from pyspark.sql import SparkSession


def local_spark(app_name: str = "llm-cost-local") -> SparkSession:
    return (
        SparkSession.builder.master("local[2]").appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
