"""Integration test: write CSV to Glue Iceberg table, then read back with DuckDB.

Requires AWS credentials and access to the atm Glue database.
Run with:
    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
    uv run pytest tests/sources/glue_iceberg/test_glue_integration.py -v -s
"""

import csv
import os
from pathlib import Path

import pytest

# skip if AWS credentials not configured
pytestmark = pytest.mark.skipif(
    not os.environ.get("AWS_ACCESS_KEY_ID"),
    reason="AWS credentials not set",
)

CSV_PATH = Path(__file__).parent / "test_data.csv"
GLUE_DATABASE = "atm"
TABLE_NAME = "t_bar_1d_test_dlt"
BUCKET_URL = "s3://atm-datalake/warehouse/"
AWS_REGION = "eu-north-1"


def _read_csv(path: Path):
    """Read CSV file and yield rows as dicts with typed values."""
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            yield {
                "symbol": row["symbol"],
                "date": row["date"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
                "ingested_at": row["ingested_at"],
            }


def test_csv_to_glue_iceberg_and_read_back():
    """Write CSV to Glue Iceberg table, then query it back with DuckDB."""
    import dlt
    import duckdb

    # configure destination via env vars
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = BUCKET_URL
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = os.environ[
        "AWS_ACCESS_KEY_ID"
    ]
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = os.environ[
        "AWS_SECRET_ACCESS_KEY"
    ]
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME"] = AWS_REGION
    os.environ["ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE"] = "glue"

    pipeline = dlt.pipeline(
        pipeline_name="test_csv_to_glue",
        destination="filesystem",
        dataset_name=GLUE_DATABASE,
    )

    # load CSV data into Glue Iceberg table
    data = dlt.resource(
        _read_csv(CSV_PATH),
        name=TABLE_NAME,
        write_disposition="replace",
    )
    load_info = pipeline.run(data, table_format="iceberg")
    print(load_info)
    assert load_info.loads_ids, "No loads completed"

    # read back with DuckDB via Glue catalog
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL iceberg; INSTALL aws; INSTALL httpfs")
    conn.execute("LOAD iceberg; LOAD aws; LOAD httpfs")
    conn.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
            REGION '{AWS_REGION}'
        )
    """)

    import boto3

    account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
    conn.execute(f"""
        ATTACH '{account_id}' AS glue_catalog (
            TYPE iceberg,
            ENDPOINT_TYPE 'glue'
        )
    """)

    rows = conn.execute(
        f"SELECT * FROM glue_catalog.{GLUE_DATABASE}.{TABLE_NAME}"
    ).fetchall()
    columns = [desc[0] for desc in conn.description]
    print(f"\nRead back {len(rows)} rows from Glue:")
    print(f"Columns: {columns}")
    for row in rows:
        print(row)

    assert len(rows) >= 5, f"Expected at least 5 rows, got {len(rows)}"
    assert "symbol" in columns
    symbols = [row[columns.index("symbol")] for row in rows]
    assert "AAPL" in symbols

    conn.close()
