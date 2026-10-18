"""Shared fixtures. Spark tests need Java 17+ (Spark 4); they are skipped if Spark cannot start."""
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from llm_cost_intelligence import data_generator as dg  # noqa: E402
from llm_cost_intelligence.config import ProjectConfig  # noqa: E402
from llm_cost_intelligence.schemas import RAW_EVENT_SCHEMA  # noqa: E402
from llm_cost_intelligence.transformations import add_bronze_metadata  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PRICING_CSV = ROOT / "config" / "model_pricing_example.csv"


@pytest.fixture(scope="session")
def spark():
    from llm_cost_intelligence.local import local_spark

    try:
        session = local_spark("llm-cost-tests")
        session.range(1).count()
    except Exception as exc:  # no Java / incompatible Java
        pytest.skip(f"Spark unavailable: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def cfg():
    return ProjectConfig()


def raw_row(**overrides) -> dict:
    """A valid raw event; override fields to create a specific case."""
    row = {
        "request_id": "req-0000001", "request_timestamp": "2025-01-10T10:00:00",
        "team": "engineering", "application": "code_assistant",
        "provider": "provider_beta", "model": "model_standard",
        "input_tokens": 1000, "output_tokens": 500, "latency_ms": 900, "status": "success",
        "retry_count": 0, "cache_hit": False, "environment": "production",
        "ingested_at": datetime(2025, 1, 10, 10, 0, 30), "batch_id": "batch_001",
    }
    row.update(overrides)
    return row


def bronze_from(spark, rows):
    df = spark.createDataFrame(dg.to_tuples(rows), RAW_EVENT_SCHEMA)
    return add_bronze_metadata(df, "test")
