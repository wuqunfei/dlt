"""
---
title: Load Iceberg tables from AWS Glue catalog
description: Load data from AWS Glue Iceberg tables into DuckDB using DuckDB's native Iceberg extension
keywords: [aws glue, iceberg, duckdb, arrow, lakehouse]
---

This example loads Iceberg tables registered in an AWS Glue catalog into a local DuckDB database.
It uses DuckDB's native Iceberg extension to read directly from S3, yielding Arrow batches for
efficient, type-preserving transfers.

We'll learn:

- How to use the `glue_iceberg` source to load all tables from a Glue database.
- How to load a single table with `glue_iceberg_table`.
- How to enable [incremental loading](../general-usage/incremental-loading) on specific tables.
- How to configure AWS credentials via config files or code.

"""

import dlt
from dlt.sources.glue_iceberg import glue_iceberg, glue_iceberg_table


def load_full_namespace() -> None:
    """Load all tables from a Glue database into DuckDB."""
    source = glue_iceberg(
        aws_account_id="123456789012",
        region="us-east-2",
        namespace="analytics",
    )

    pipeline = dlt.pipeline(
        pipeline_name="glue_to_duckdb",
        destination="duckdb",
        dataset_name="analytics",
    )

    load_info = pipeline.run(source)
    print(load_info)


def load_specific_tables() -> None:
    """Load only specific tables by name."""
    source = glue_iceberg(
        aws_account_id="123456789012",
        region="us-east-2",
        table_names=["analytics.orders", "analytics.customers"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="glue_to_duckdb",
        destination="duckdb",
        dataset_name="analytics",
    )

    load_info = pipeline.run(source)
    print(load_info)


def load_with_incremental() -> None:
    """Load tables with incremental loading on a cursor column."""
    source = glue_iceberg(
        aws_account_id="123456789012",
        region="us-east-2",
        namespace="analytics",
    )

    # opt specific tables into incremental loading
    source.orders.apply_hints(incremental=dlt.sources.incremental("updated_at"))
    source.customers.apply_hints(incremental=dlt.sources.incremental("modified_at"))

    pipeline = dlt.pipeline(
        pipeline_name="glue_incremental",
        destination="duckdb",
        dataset_name="analytics",
    )

    load_info = pipeline.run(source)
    print(load_info)


def load_single_table() -> None:
    """Load a single Glue Iceberg table as a standalone resource."""
    resource = glue_iceberg_table(
        aws_account_id="123456789012",
        region="us-east-2",
        namespace="analytics",
        table="orders",
    )

    pipeline = dlt.pipeline(
        pipeline_name="glue_single_table",
        destination="duckdb",
        dataset_name="analytics",
    )

    load_info = pipeline.run(resource)
    print(load_info)


def load_with_role_assumption() -> None:
    """Load using STS assume-role for cross-account access."""
    source = glue_iceberg(
        aws_account_id="123456789012",
        region="us-east-2",
        namespace="analytics",
        role_arn="arn:aws:iam::123456789012:role/glue-reader",
    )

    pipeline = dlt.pipeline(
        pipeline_name="glue_cross_account",
        destination="duckdb",
        dataset_name="analytics",
    )

    load_info = pipeline.run(source)
    print(load_info)


if __name__ == "__main__":
    load_full_namespace()
