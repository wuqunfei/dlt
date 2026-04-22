from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_glue_connection():
    with patch("dlt.sources.glue_iceberg.helpers.duckdb") as mock_duckdb:
        conn = MagicMock()
        mock_duckdb.connect.return_value = conn
        # discover_tables returns two tables
        conn.execute.return_value.fetchall.return_value = [
            ("orders",),
            ("customers",),
        ]
        # fetch_arrow_table returns empty immediately (no data in test)
        empty = MagicMock()
        empty.__len__ = lambda self: 0
        conn.execute.return_value.fetch_arrow_table.return_value = empty
        yield conn


def test_glue_iceberg_source_discovers_tables(mock_glue_connection):
    from dlt.sources.glue_iceberg import glue_iceberg

    source = glue_iceberg(
        aws_account_id="123456789012",
        region="us-east-2",
        namespace="analytics",
    )
    resource_names = list(source.resources.keys())
    assert "orders" in resource_names
    assert "customers" in resource_names
    assert len(resource_names) == 2


def test_glue_iceberg_source_explicit_tables(mock_glue_connection):
    from dlt.sources.glue_iceberg import glue_iceberg

    source = glue_iceberg(
        aws_account_id="123456789012",
        region="us-east-2",
        table_names=["analytics.orders"],
    )
    resource_names = list(source.resources.keys())
    assert len(resource_names) == 1
    assert "orders" in resource_names


def test_glue_iceberg_source_requires_namespace_or_tables():
    from dlt.sources.glue_iceberg import glue_iceberg

    with pytest.raises(ValueError, match="namespace.*table_names"):
        list(
            glue_iceberg(
                aws_account_id="123456789012",
                region="us-east-2",
            )
        )


def test_glue_iceberg_table_resource(mock_glue_connection):
    from dlt.sources.glue_iceberg import glue_iceberg_table

    resource = glue_iceberg_table(
        aws_account_id="123456789012",
        region="us-east-2",
        namespace="analytics",
        table="orders",
    )
    assert resource.name == "orders"
