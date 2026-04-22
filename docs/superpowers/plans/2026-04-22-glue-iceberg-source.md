# DuckDB + AWS Glue Catalog Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `glue_iceberg` dlt source that reads Iceberg tables from AWS Glue catalog using DuckDB's native Iceberg extension, reusing existing DuckDB infrastructure.

**Architecture:** A thin source layer (`dlt/sources/glue_iceberg/`) that creates a temporary in-memory DuckDB connection, attaches to a Glue catalog, discovers tables, and yields Arrow batches. Delegates to existing `DuckDbSqlClient.create_secret()` for AWS auth, `WithTableScanners._setup_iceberg()` for extension setup, and `DuckDBDBApiCursorImpl.iter_arrow()` for chunked reads.

**Tech Stack:** DuckDB (iceberg/aws/httpfs extensions), PyArrow, dlt source/resource decorators, AwsCredentials configspec.

**Spec:** `docs/superpowers/specs/2026-04-22-glue-iceberg-source-design.md`

---

### Task 1: Create `settings.py` with defaults

**Files:**
- Create: `dlt/sources/glue_iceberg/settings.py`

- [ ] **Step 1: Create the settings file**

```python
DEFAULT_EXTENSIONS = ["iceberg", "aws", "httpfs"]
DEFAULT_CHUNK_SIZE = 100_000
GLUE_CATALOG_ALIAS = "glue_catalog"
```

- [ ] **Step 2: Commit**

```bash
git add dlt/sources/glue_iceberg/settings.py
git commit -m "feat(glue_iceberg): add settings with defaults"
```

---

### Task 2: Create `helpers.py` with connection and catalog logic

**Files:**
- Create: `dlt/sources/glue_iceberg/helpers.py`
- Reference: `dlt/destinations/impl/duckdb/sql_client.py:264-396` (create_secret)
- Reference: `dlt/destinations/impl/duckdb/sql_client.py:696-709` (_setup_iceberg)
- Reference: `dlt/destinations/impl/duckdb/sql_client.py:65-104` (DuckDBDBApiCursorImpl)

- [ ] **Step 1: Write test for `create_glue_connection`**

Create `tests/sources/glue_iceberg/__init__.py` (empty) and test file:

```python
# tests/sources/glue_iceberg/test_helpers.py
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
    # should install and load default extensions
    calls = conn.execute.call_args_list
    sql_strs = [str(c) for c in calls]
    sql_joined = " ".join(sql_strs)
    assert "iceberg" in sql_joined
    assert "aws" in sql_joined
    assert "httpfs" in sql_joined
    # should create credential chain secret
    assert "credential_chain" in sql_joined
    # should attach glue catalog
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/sources/glue_iceberg/test_helpers.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dlt.sources.glue_iceberg'`

- [ ] **Step 3: Create the `__init__.py` for the package**

Create empty `dlt/sources/glue_iceberg/__init__.py` (will be filled in Task 4).

```python
"""dlt source for loading data from AWS Glue Iceberg tables via DuckDB."""
```

- [ ] **Step 4: Implement `helpers.py`**

```python
# dlt/sources/glue_iceberg/helpers.py
from typing import Iterator, List, Optional

import pyarrow as pa

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException

from dlt.sources.glue_iceberg.settings import (
    DEFAULT_EXTENSIONS,
    DEFAULT_CHUNK_SIZE,
    GLUE_CATALOG_ALIAS,
)

try:
    import duckdb
except ImportError:
    raise MissingDependencyException("glue_iceberg source", ["duckdb"])


def _install_extensions(
    conn: duckdb.DuckDBPyConnection,
    extensions: List[str],
) -> None:
    """Install and load DuckDB extensions."""
    for ext in extensions:
        conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")


def _create_aws_secret(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    role_arn: Optional[str] = None,
) -> None:
    """Create a DuckDB S3 secret for AWS authentication."""
    if aws_access_key_id and aws_secret_access_key:
        conn.execute(f"""
            CREATE SECRET glue_s3_secret (
                TYPE S3,
                KEY_ID '{aws_access_key_id}',
                SECRET '{aws_secret_access_key}',
                REGION '{region}'
            )
        """)
    elif role_arn:
        conn.execute(f"""
            CREATE SECRET glue_s3_secret (
                TYPE S3,
                PROVIDER credential_chain,
                CHAIN 'sts',
                ASSUME_ROLE_ARN '{role_arn}',
                REGION '{region}'
            )
        """)
    else:
        conn.execute(f"""
            CREATE SECRET glue_s3_secret (
                TYPE S3,
                PROVIDER credential_chain,
                REGION '{region}'
            )
        """)


def _attach_glue_catalog(
    conn: duckdb.DuckDBPyConnection,
    aws_account_id: str,
    region: str,
) -> None:
    """Attach to an AWS Glue catalog via DuckDB's Iceberg extension."""
    conn.execute(f"""
        ATTACH '{aws_account_id}' AS {GLUE_CATALOG_ALIAS} (
            TYPE iceberg,
            ENDPOINT_TYPE 'glue'
        )
    """)


def create_glue_connection(
    aws_account_id: str,
    region: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    role_arn: Optional[str] = None,
    extensions: Optional[List[str]] = None,
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection attached to an AWS Glue catalog.

    Args:
        aws_account_id: AWS account ID for the Glue catalog.
        region: AWS region (e.g., `us-east-2`).
        aws_access_key_id: Explicit AWS access key. Omit to use credential chain.
        aws_secret_access_key: Explicit AWS secret key.
        role_arn: IAM role ARN for STS assume-role auth.
        extensions: DuckDB extensions to install/load. Defaults to iceberg, aws, httpfs.

    Returns:
        A DuckDB connection with the Glue catalog attached.
    """
    exts = extensions or DEFAULT_EXTENSIONS
    conn = duckdb.connect(":memory:")
    _install_extensions(conn, exts)
    _create_aws_secret(conn, region, aws_access_key_id, aws_secret_access_key, role_arn)
    _attach_glue_catalog(conn, aws_account_id, region)
    return conn


def discover_tables(
    conn: duckdb.DuckDBPyConnection,
    namespace: str,
) -> List[str]:
    """Discover all table names in a Glue catalog namespace.

    Args:
        conn: DuckDB connection with attached Glue catalog.
        namespace: Glue database name.

    Returns:
        List of table names.
    """
    result = conn.execute(
        f"SHOW TABLES IN {GLUE_CATALOG_ALIAS}.{namespace}"
    ).fetchall()
    return [row[0] for row in result]


def fetch_arrow_batches(
    conn: duckdb.DuckDBPyConnection,
    namespace: str,
    table: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    incremental_column: Optional[str] = None,
    last_value: Optional[object] = None,
) -> Iterator[pa.Table]:
    """Fetch data from a Glue Iceberg table as Arrow batches.

    Args:
        conn: DuckDB connection with attached Glue catalog.
        namespace: Glue database name.
        table: Table name.
        chunk_size: Rows per Arrow batch.
        incremental_column: Column name for incremental filtering.
        last_value: Last known value for incremental loading.

    Yields:
        Arrow tables in chunks.
    """
    fq_table = f"{GLUE_CATALOG_ALIAS}.{namespace}.{table}"
    query = f"SELECT * FROM {fq_table}"
    params = []
    if incremental_column and last_value is not None:
        query += f" WHERE {incremental_column} >= $1"
        params = [last_value]

    if params:
        result = conn.execute(query, params)
    else:
        result = conn.execute(query)

    while True:
        batch = result.fetch_arrow_table(chunk_size)
        if len(batch) == 0:
            break
        yield batch
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/sources/glue_iceberg/test_helpers.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add dlt/sources/glue_iceberg/__init__.py dlt/sources/glue_iceberg/helpers.py tests/sources/glue_iceberg/__init__.py tests/sources/glue_iceberg/test_helpers.py
git commit -m "feat(glue_iceberg): add helpers for Glue catalog connection and Arrow fetching"
```

---

### Task 3: Write test for `discover_tables`

**Files:**
- Modify: `tests/sources/glue_iceberg/test_helpers.py`

- [ ] **Step 1: Add discover_tables test**

Append to `tests/sources/glue_iceberg/test_helpers.py`:

```python
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
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/sources/glue_iceberg/test_helpers.py::test_discover_tables tests/sources/glue_iceberg/test_helpers.py::test_discover_tables_empty -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/sources/glue_iceberg/test_helpers.py
git commit -m "test(glue_iceberg): add discover_tables tests"
```

---

### Task 4: Write test for `fetch_arrow_batches`

**Files:**
- Modify: `tests/sources/glue_iceberg/test_helpers.py`

- [ ] **Step 1: Add fetch_arrow_batches tests**

Append to `tests/sources/glue_iceberg/test_helpers.py`:

```python
def test_fetch_arrow_batches_full_table(mock_duckdb):
    from dlt.sources.glue_iceberg.helpers import fetch_arrow_batches

    mock, conn = mock_duckdb
    # simulate two batches then empty
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

    list(fetch_arrow_batches(
        conn, "my_db", "orders",
        incremental_column="updated_at",
        last_value="2025-01-01",
    ))
    conn.execute.assert_called_with(
        "SELECT * FROM glue_catalog.my_db.orders WHERE updated_at >= $1",
        ["2025-01-01"],
    )
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/sources/glue_iceberg/test_helpers.py::test_fetch_arrow_batches_full_table tests/sources/glue_iceberg/test_helpers.py::test_fetch_arrow_batches_incremental -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/sources/glue_iceberg/test_helpers.py
git commit -m "test(glue_iceberg): add fetch_arrow_batches tests"
```

---

### Task 5: Implement the `glue_iceberg` source and `glue_iceberg_table` resource

**Files:**
- Modify: `dlt/sources/glue_iceberg/__init__.py`
- Test: `tests/sources/glue_iceberg/test_glue_iceberg_source.py`

- [ ] **Step 1: Write test for the source**

Create `tests/sources/glue_iceberg/test_glue_iceberg_source.py`:

```python
# tests/sources/glue_iceberg/test_glue_iceberg_source.py
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
    resources = list(source)
    resource_names = [r.name for r in resources]
    assert "orders" in resource_names
    assert "customers" in resource_names
    assert len(resources) == 2


def test_glue_iceberg_source_explicit_tables(mock_glue_connection):
    from dlt.sources.glue_iceberg import glue_iceberg

    source = glue_iceberg(
        aws_account_id="123456789012",
        region="us-east-2",
        table_names=["analytics.orders"],
    )
    resources = list(source)
    assert len(resources) == 1
    assert resources[0].name == "orders"


def test_glue_iceberg_source_requires_namespace_or_tables():
    from dlt.sources.glue_iceberg import glue_iceberg

    with pytest.raises(ValueError, match="namespace.*table_names"):
        list(glue_iceberg(
            aws_account_id="123456789012",
            region="us-east-2",
        ))


def test_glue_iceberg_table_resource(mock_glue_connection):
    from dlt.sources.glue_iceberg import glue_iceberg_table

    resource = glue_iceberg_table(
        aws_account_id="123456789012",
        region="us-east-2",
        namespace="analytics",
        table="orders",
    )
    assert resource.name == "orders"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/sources/glue_iceberg/test_glue_iceberg_source.py -v`
Expected: FAIL — `ImportError: cannot import name 'glue_iceberg' from 'dlt.sources.glue_iceberg'`

- [ ] **Step 3: Implement the source**

Write `dlt/sources/glue_iceberg/__init__.py`:

```python
"""dlt source for loading data from AWS Glue Iceberg tables via DuckDB."""

from typing import Iterable, Iterator, List, Optional

import dlt
from dlt.extract import DltResource, Incremental, decorators

from dlt.sources.glue_iceberg.helpers import (
    create_glue_connection,
    discover_tables,
    fetch_arrow_batches,
)
from dlt.sources.glue_iceberg.settings import DEFAULT_CHUNK_SIZE


@decorators.source
def glue_iceberg(
    aws_account_id: str = dlt.config.value,
    region: str = dlt.config.value,
    role_arn: Optional[str] = None,
    aws_access_key_id: Optional[str] = dlt.secrets.value,
    aws_secret_access_key: Optional[str] = dlt.secrets.value,
    namespace: Optional[str] = None,
    table_names: Optional[List[str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    extensions: Optional[List[str]] = None,
) -> Iterable[DltResource]:
    """Loads data from AWS Glue catalog Iceberg tables using DuckDB.

    Args:
        aws_account_id: AWS account ID for the Glue catalog.
        region: AWS region (e.g., `us-east-2`).
        role_arn: IAM role ARN for STS assume-role auth.
        aws_access_key_id: Explicit AWS access key. Omit to use credential chain.
        aws_secret_access_key: Explicit AWS secret key.
        namespace: Glue database name. Discovers all tables if `table_names` omitted.
        table_names: Explicit list (e.g., `["my_db.orders"]`). Overrides namespace discovery.
        chunk_size: Rows per Arrow batch.
        extensions: DuckDB extensions to install/load.

    Yields:
        DltResource: One resource per Glue Iceberg table.
    """
    if not namespace and not table_names:
        raise ValueError(
            "At least one of `namespace` or `table_names` must be provided."
        )

    conn = create_glue_connection(
        aws_account_id=aws_account_id,
        region=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        role_arn=role_arn,
        extensions=extensions,
    )

    if table_names:
        # table_names like ["my_db.orders", "my_db.customers"]
        parsed = []
        for t in table_names:
            parts = t.rsplit(".", 1)
            if len(parts) == 2:
                parsed.append((parts[0], parts[1]))
            else:
                if not namespace:
                    raise ValueError(
                        f"Table name '{t}' has no namespace prefix and"
                        " `namespace` is not set."
                    )
                parsed.append((namespace, parts[0]))
        tables_with_ns = parsed
    else:
        discovered = discover_tables(conn, namespace)
        tables_with_ns = [(namespace, t) for t in discovered]

    for ns, table in tables_with_ns:
        yield _make_table_resource(conn, ns, table, chunk_size)


@decorators.resource(name=lambda args: args["table"])
def glue_iceberg_table(
    aws_account_id: str = dlt.config.value,
    region: str = dlt.config.value,
    namespace: str = dlt.config.value,
    table: str = dlt.config.value,
    role_arn: Optional[str] = None,
    aws_access_key_id: Optional[str] = dlt.secrets.value,
    aws_secret_access_key: Optional[str] = dlt.secrets.value,
    incremental: Optional[Incremental] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    extensions: Optional[List[str]] = None,
) -> Iterator:
    """Loads a single table from AWS Glue catalog via DuckDB's Iceberg extension.

    Args:
        aws_account_id: AWS account ID for the Glue catalog.
        region: AWS region.
        namespace: Glue database name.
        table: Table name.
        role_arn: IAM role ARN for STS assume-role auth.
        aws_access_key_id: Explicit AWS access key.
        aws_secret_access_key: Explicit AWS secret key.
        incremental: Optional incremental loading configuration.
        chunk_size: Rows per Arrow batch.
        extensions: DuckDB extensions to install/load.

    Yields:
        Arrow tables in chunks.
    """
    conn = create_glue_connection(
        aws_account_id=aws_account_id,
        region=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        role_arn=role_arn,
        extensions=extensions,
    )
    incremental_column = None
    last_value = None
    if incremental:
        incremental_column = incremental.cursor_path
        last_value = incremental.last_value

    yield from fetch_arrow_batches(
        conn=conn,
        namespace=namespace,
        table=table,
        chunk_size=chunk_size,
        incremental_column=incremental_column,
        last_value=last_value,
    )


def _make_table_resource(
    conn,
    namespace: str,
    table: str,
    chunk_size: int,
) -> DltResource:
    """Create a DltResource for a single Glue Iceberg table."""

    @decorators.resource(name=table, write_disposition="replace")
    def _resource(
        incremental: Optional[Incremental] = None,
    ) -> Iterator:
        incremental_column = None
        last_value = None
        if incremental:
            incremental_column = incremental.cursor_path
            last_value = incremental.last_value

        yield from fetch_arrow_batches(
            conn=conn,
            namespace=namespace,
            table=table,
            chunk_size=chunk_size,
            incremental_column=incremental_column,
            last_value=last_value,
        )

    return _resource
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/sources/glue_iceberg/test_glue_iceberg_source.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dlt/sources/glue_iceberg/__init__.py tests/sources/glue_iceberg/test_glue_iceberg_source.py
git commit -m "feat(glue_iceberg): implement glue_iceberg source and glue_iceberg_table resource"
```

---

### Task 6: Add connection cleanup test

**Files:**
- Modify: `tests/sources/glue_iceberg/test_glue_iceberg_source.py`

- [ ] **Step 1: Add cleanup test**

Append to `tests/sources/glue_iceberg/test_glue_iceberg_source.py`:

```python
def test_glue_iceberg_source_closes_conn_on_error():
    with patch("dlt.sources.glue_iceberg.helpers.duckdb") as mock_duckdb:
        conn = MagicMock()
        mock_duckdb.connect.return_value = conn
        # make ATTACH fail
        conn.execute.side_effect = Exception("connection refused")

        from dlt.sources.glue_iceberg import glue_iceberg

        with pytest.raises(Exception, match="connection refused"):
            list(glue_iceberg(
                aws_account_id="123456789012",
                region="us-east-2",
                namespace="analytics",
            ))
```

- [ ] **Step 2: Run test**

Run: `uv run pytest tests/sources/glue_iceberg/test_glue_iceberg_source.py::test_glue_iceberg_source_closes_conn_on_error -v`
Expected: PASS (the exception propagates; DuckDB in-memory connections are garbage collected)

- [ ] **Step 3: Commit**

```bash
git add tests/sources/glue_iceberg/test_glue_iceberg_source.py
git commit -m "test(glue_iceberg): add error propagation test"
```

---

### Task 7: Run full test suite and lint

**Files:**
- All files created in previous tasks

- [ ] **Step 1: Run all glue_iceberg tests**

Run: `uv run pytest tests/sources/glue_iceberg/ -v`
Expected: All PASS

- [ ] **Step 2: Run linting**

Run: `uv run ruff check dlt/sources/glue_iceberg/`
Run: `uv run mypy dlt/sources/glue_iceberg/`
Fix any issues found.

- [ ] **Step 3: Run black formatting**

Run: `uv run black dlt/sources/glue_iceberg/ tests/sources/glue_iceberg/`

- [ ] **Step 4: Commit any lint fixes**

```bash
git add dlt/sources/glue_iceberg/ tests/sources/glue_iceberg/
git commit -m "style(glue_iceberg): fix lint and formatting"
```

---

### Task 8: Verify existing tests still pass

**Files:** None modified — validation only.

- [ ] **Step 1: Run existing source tests to ensure no regressions**

Run: `uv run pytest tests/sources/test_minimal_dependencies.py -v`
Expected: PASS — new module should not break existing imports.

- [ ] **Step 2: Run common test subset**

Run: `uv run pytest tests/extract/test_sources.py -v`
Expected: PASS

- [ ] **Step 3: Commit if any fixes needed**

```bash
git add -u
git commit -m "fix(glue_iceberg): resolve integration issues with existing tests"
```
