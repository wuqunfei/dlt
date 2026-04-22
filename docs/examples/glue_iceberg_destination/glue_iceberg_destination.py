"""
---
title: Write Iceberg tables to AWS Glue catalog
description: Load CSV and other data into AWS Glue Iceberg tables using dlt's filesystem destination
keywords: [aws glue, iceberg, duckdb, csv, lakehouse, filesystem]
---

Write data into Iceberg tables registered in AWS Glue catalog using dlt's
filesystem destination with `iceberg_catalog_type = "glue"`.

Install dependencies:
```sh
pip install "dlt[filesystem,pyiceberg-glue]"
```

We'll learn:

- How to configure the filesystem destination to use AWS Glue as the Iceberg catalog.
- How to load a CSV file into a Glue Iceberg table.
- How to query Glue Iceberg tables with DuckDB after loading.

## Configuration

config.toml:
```toml
[destination.filesystem]
bucket_url = "s3://atm-datalake/warehouse/"

[iceberg_catalog]
iceberg_catalog_type = "glue"
```

secrets.toml:
```toml
[destination.filesystem.credentials]
aws_access_key_id = "AKIA..."
aws_secret_access_key = "..."
region_name = "eu-north-1"
```

"""

import csv
from pathlib import Path
from typing import Iterator

import dlt


def read_csv(path: str) -> Iterator[dict]:
    """Read a CSV file and yield rows as typed dicts."""
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            # cast numeric columns so dlt infers correct types
            yield {k: _try_cast(v) for k, v in row.items()}


def _try_cast(value: str):
    """Try to cast string to int or float, otherwise return as-is."""
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def csv_to_glue_iceberg() -> None:
    """Load a CSV file into a Glue Iceberg table.

    Writes CSV data to S3 as Iceberg format and registers the table
    in the AWS Glue catalog. The table is created at:
    Glue database "atm" -> table "transactions"
    """
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_glue",
        destination="filesystem",
        dataset_name="atm",
    )

    data = dlt.resource(
        read_csv("transactions.csv"),
        name="transactions",
        write_disposition="replace",
    )

    load_info = pipeline.run(data, table_format="iceberg")
    print(load_info)


def csv_to_glue_then_query_with_duckdb() -> None:
    """Load a CSV into Glue Iceberg, then query it with DuckDB.

    Step 1: Write CSV to S3 as Iceberg, register in Glue catalog.
    Step 2: Attach DuckDB to the Glue catalog and query the table.
    """
    import duckdb

    # step 1: load CSV into Glue Iceberg table
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_glue",
        destination="filesystem",
        dataset_name="atm",
    )

    data = dlt.resource(
        read_csv("transactions.csv"),
        name="transactions",
        write_disposition="replace",
    )
    load_info = pipeline.run(data, table_format="iceberg")
    print(load_info)

    # step 2: query with DuckDB via Glue catalog
    import boto3

    aws_account_id = boto3.client("sts").get_caller_identity()["Account"]

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL iceberg; INSTALL aws; INSTALL httpfs")
    conn.execute("LOAD iceberg; LOAD aws; LOAD httpfs")
    conn.execute(
        """
        CREATE SECRET (
            TYPE S3,
            PROVIDER credential_chain
        )
    """
    )
    conn.execute(
        f"""
        ATTACH '{aws_account_id}' AS glue_catalog (
            TYPE iceberg,
            ENDPOINT_TYPE 'glue'
        )
    """
    )

    rows = conn.execute("SELECT * FROM glue_catalog.atm.transactions").fetchall()
    columns = [desc[0] for desc in conn.description]
    print(f"Columns: {columns}")
    for row in rows:
        print(row)

    conn.close()


def multiple_csv_to_glue() -> None:
    """Load multiple CSV files into separate Glue Iceberg tables."""
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_glue",
        destination="filesystem",
        dataset_name="atm",
    )

    orders = dlt.resource(
        read_csv("orders.csv"),
        name="orders",
        write_disposition="replace",
    )
    customers = dlt.resource(
        read_csv("customers.csv"),
        name="customers",
        write_disposition="replace",
    )

    # creates atm.orders and atm.customers in Glue
    load_info = pipeline.run([orders, customers], table_format="iceberg")
    print(load_info)


if __name__ == "__main__":
    csv_to_glue_iceberg()
