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

import dlt


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

    data = dlt.resource(name="transactions").from_csv("transactions.csv")

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

    data = dlt.resource(name="transactions").from_csv("transactions.csv")
    load_info = pipeline.run(data, table_format="iceberg")
    print(load_info)

    # step 2: query with DuckDB via Glue catalog
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL iceberg; INSTALL aws; INSTALL httpfs")
    conn.execute("LOAD iceberg; LOAD aws; LOAD httpfs")
    conn.execute("""
        CREATE SECRET (
            TYPE S3,
            PROVIDER credential_chain,
            REGION 'eu-north-1'
        )
    """)
    conn.execute("""
        ATTACH '123456789012' AS glue_catalog (
            TYPE iceberg,
            ENDPOINT_TYPE 'glue'
        )
    """)

    df = conn.execute("SELECT * FROM glue_catalog.atm.transactions").fetchdf()
    print(df)


def multiple_csv_to_glue() -> None:
    """Load multiple CSV files into separate Glue Iceberg tables."""
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_glue",
        destination="filesystem",
        dataset_name="atm",
    )

    orders = dlt.resource(name="orders").from_csv("orders.csv")
    customers = dlt.resource(name="customers").from_csv("customers.csv")

    # creates atm.orders and atm.customers in Glue
    load_info = pipeline.run([orders, customers], table_format="iceberg")
    print(load_info)


if __name__ == "__main__":
    csv_to_glue_iceberg()
