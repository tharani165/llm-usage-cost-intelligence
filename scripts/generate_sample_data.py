"""Write generated batches to local CSV files (no Spark needed).

Usage: python scripts/generate_sample_data.py --rows 100 --out sample_data
"""
import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from llm_cost_intelligence import data_generator as dg  # noqa: E402


def write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=dg.BATCH_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="sample_data")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rows1, expected1 = dg.generate_initial_batch(dg.GeneratorConfig(args.rows, args.seed))
    rows2, expected2 = dg.generate_incremental_batch(rows1)
    write_csv(out / "batch_001.csv", rows1)
    write_csv(out / "batch_002.csv", rows2)
    print("batch_001:", len(rows1), "rows | expected:", expected1)
    print("batch_002:", len(rows2), "rows | expected:", {k: v for k, v in expected2.items() if k != "corrected_request_ids"})


if __name__ == "__main__":
    main()
