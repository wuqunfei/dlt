from unittest.mock import MagicMock, patch, call
import pytest


@pytest.fixture
def mock_duckdb():
    with patch("dlt.sources.glue_iceberg.helpers.duckdb") as mock:
        conn = MagicMock()
        mock.connect.return_value = conn
        yield mock, conn


def test_create_glue_connection_with_credential_chain(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import create_glue_connection

    mock, conn = mock_duckdb
    result = create_glue_connection(
        aws_account_id="123456789012",
        region="us-east-2",
    )
    assert result is conn
    calls = conn.execute.call_args_list
    sql_strs = [str(c) for c in calls]
    sql_joined = " ".join(sql_strs)
    assert "iceberg" in sql_joined
    assert "aws" in sql_joined
    assert "httpfs" in sql_joined
    assert "credential_chain" in sql_joined
    assert "ATTACH" in sql_joined
    assert "123456789012" in sql_joined


def test_create_glue_connection_with_explicit_creds(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import create_glue_connection

    mock, conn = mock_duckdb
    result = create_glue_connection(
        aws_account_id="123456789012",
        region="us-east-2",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    assert result is conn
    calls = conn.execute.call_args_list
    sql_joined = " ".join(str(c) for c in calls)
    assert "AKIAIOSFODNN7EXAMPLE" in sql_joined
    assert "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" in sql_joined


def test_create_glue_connection_with_role_arn(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import create_glue_connection

    mock, conn = mock_duckdb
    result = create_glue_connection(
        aws_account_id="123456789012",
        region="us-east-2",
        role_arn="arn:aws:iam::123456789012:role/my-role",
    )
    calls = conn.execute.call_args_list
    sql_joined = " ".join(str(c) for c in calls)
    assert "ASSUME_ROLE_ARN" in sql_joined
    assert "arn:aws:iam::123456789012:role/my-role" in sql_joined


def test_create_glue_connection_custom_extensions(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import create_glue_connection

    mock, conn = mock_duckdb
    result = create_glue_connection(
        aws_account_id="123456789012",
        region="us-east-2",
        extensions=["iceberg", "aws", "httpfs", "spatial"],
    )
    calls = conn.execute.call_args_list
    sql_joined = " ".join(str(c) for c in calls)
    assert "spatial" in sql_joined


def test_discover_tables(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import discover_tables

    mock, conn = mock_duckdb
    conn.execute.return_value.fetchall.return_value = [
        ("orders",),
        ("customers",),
        ("products",),
    ]
    tables = discover_tables(conn, "my_database")
    assert tables == ["orders", "customers", "products"]
    conn.execute.assert_called_with("SHOW TABLES IN glue_catalog.my_database")


def test_discover_tables_empty(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import discover_tables

    mock, conn = mock_duckdb
    conn.execute.return_value.fetchall.return_value = []
    tables = discover_tables(conn, "empty_db")
    assert tables == []


def test_fetch_arrow_batches_full_table(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import fetch_arrow_batches

    mock, conn = mock_duckdb
    batch1 = MagicMock()
    batch1.__len__ = lambda self: 100
    batch2 = MagicMock()
    batch2.__len__ = lambda self: 50
    empty = MagicMock()
    empty.__len__ = lambda self: 0
    conn.execute.return_value.fetch_arrow_table.side_effect = [batch1, batch2, empty]

    batches = list(fetch_arrow_batches(conn, "my_db", "orders", chunk_size=100))
    assert len(batches) == 2
    conn.execute.assert_called_with("SELECT * FROM glue_catalog.my_db.orders")


def test_fetch_arrow_batches_incremental(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import fetch_arrow_batches

    mock, conn = mock_duckdb
    empty = MagicMock()
    empty.__len__ = lambda self: 0
    conn.execute.return_value.fetch_arrow_table.side_effect = [empty]

    list(
        fetch_arrow_batches(
            conn,
            "my_db",
            "orders",
            incremental_column="updated_at",
            last_value="2025-01-01",
        )
    )
    conn.execute.assert_called_with(
        "SELECT * FROM glue_catalog.my_db.orders WHERE updated_at >= $1",
        ["2025-01-01"],
    )
