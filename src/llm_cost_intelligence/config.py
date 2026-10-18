"""Project configuration: catalog/schema, table names, thresholds, synthetic budgets."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

TABLE_NAMES = {
    "landing": "landing_usage_events",
    "bronze_events": "bronze_usage_events",
    "bronze_pricing": "bronze_model_pricing",
    "silver_pricing": "silver_model_pricing",
    "silver_events": "silver_usage_events",
    "quarantine": "quarantined_usage_events",
    "priced_events": "silver_priced_usage_events",
    "gold_usage_daily": "gold_usage_daily",
    "gold_model_performance": "gold_model_performance",
    "gold_budget_utilization": "gold_budget_utilization",
    "gold_quality_metrics": "gold_quality_metrics",
}

DEFAULT_BUDGETS = {  # synthetic monthly budgets, illustrative currency units
    "support_assistant": 40.0,
    "code_assistant": 60.0,
    "document_summarizer": 25.0,
    "internal_knowledge_assistant": 30.0,
}


@dataclass
class ProjectConfig:
    catalog: str = "workspace"
    schema: str = "llm_usage"
    n_requests: int = 20_000
    seed: int = 42
    high_latency_ms: int = 30_000
    high_input_tokens: int = 20_000
    high_output_tokens: int = 8_000
    high_retry_count: int = 3
    late_arrival_hours: int = 24
    budget_warning_ratio: float = 0.8
    monthly_budgets: dict = field(default_factory=lambda: dict(DEFAULT_BUDGETS))

    def __post_init__(self) -> None:
        if not 1 <= self.n_requests <= 50_000:
            raise ValueError("n_requests must be between 1 and 50000")

    def table(self, key: str) -> str:
        """Fully qualified, backtick-quoted table name for a logical table key."""
        return f"`{self.catalog}`.`{self.schema}`.`{TABLE_NAMES[key]}`"


def load_config(path: str | Path | None = None, **overrides) -> ProjectConfig:
    """Defaults, optionally overlaid by a YAML file, then by keyword overrides."""
    values: dict = {}
    if path and Path(path).exists():
        import yaml  # only needed when a YAML file is used

        values.update(yaml.safe_load(Path(path).read_text()) or {})
    values.update({k: v for k, v in overrides.items() if v is not None})
    return ProjectConfig(**values)
