# Design: DuckDB + AWS Glue Catalog Support for dlt

## Overview

Adds AWS Glue catalog support to dlt's existing DuckDB ecosystem. This enables users to load data from Iceberg tables registered in AWS Glue into any dlt destination, using DuckDB's native Iceberg extension as the read engine.

This is an extension of dlt's DuckDB infrastructure — not a standalone project. It builds on:
- `DuckDbSqlClient.create_secret()` for AWS credential management
- `WithTableScanners._setup_iceberg()` for Iceberg extension setup
- `DuckDbConnectionPool` for extension loading and connection config
- `DuckDBDBApiCursorImpl.iter_arrow()` for chunked Arrow batch iteration

## Data Flow

```
AWS Glue Catalog (Iceberg tables on S3)
    ↓  DuckDB ATTACH (iceberg extension)
Temporary in-memory DuckDB connection (reusing existing dlt DuckDB internals)
    ↓  SELECT * → iter_arrow()
Arrow batches (pa.Table)
    ↓  dlt resource yield
dlt pipeline → any destination (DuckDB, Postgres, BigQuery, etc.)
```

- The read-side DuckDB connection is **separate** from any DuckDB destination — it is a temporary in-memory scratchpad, created and destroyed per run.
- Data transfers as Arrow tables for efficiency and type fidelity.

## Existing DuckDB Code Reused

| Existing code | Location | What we reuse |
|---------------|----------|---------------|
| `DuckDbSqlClient.create_secret()` | `dlt/destinations/impl/duckdb/sql_client.py:264` | S3 secret creation (credential_chain, explicit keys, STS) |
| `WithTableScanners._setup_iceberg()` | `dlt/destinations/impl/duckdb/sql_client.py:696` | Iceberg extension install/load with version handling |
| `DuckDbConnectionPool` | `dlt/destinations/impl/duckdb/configuration.py:74` | Extension loading, config, pragmas |
| `DuckDBDBApiCursorImpl.iter_arrow()` | `dlt/destinations/impl/duckdb/sql_client.py:65` | Chunked Arrow batch iteration |
| `AwsCredentials` | `dlt/common/configuration/specs/aws_credentials.py` | AWS credential spec with credential chain support |

## Public API

### Source: `glue_iceberg()`

```python
@dlt.source
def glue_iceberg(
    aws_account_id: str = dlt.config.value,
    region: str = dlt.config.value,
    role_arn: Optional[str] = dlt.config.value,
    aws_access_key_id: Optional[str] = dlt.secrets.value,
    aws_secret_access_key: Optional[str] = dlt.secrets.value,
    namespace: Optional[str] = dlt.config.value,
    table_names: Optional[List[str]] = dlt.config.value,
    chunk_size: int = 100_000,
    extensions: Optional[List[str]] = None,
) -> Iterable[DltResource]:
```

**Parameters:**

| Parameter | Required | Source | Description |
|-----------|----------|--------|-------------|
| `aws_account_id` | yes | config | AWS account ID for Glue catalog |
| `region` | yes | config | AWS region (e.g., `us-east-2`) |
| `role_arn` | no | config | IAM role ARN for STS assume-role auth |
| `aws_access_key_id` | no | secrets | Explicit AWS access key; omit to use credential chain |
| `aws_secret_access_key` | no | secrets | Explicit AWS secret key |
| `namespace` | no | config | Glue database name; discovers all tables if `table_names` omitted |
| `table_names` | no | config | Explicit table list (e.g., `["my_db.orders"]`); overrides namespace discovery |
| `chunk_size` | no | config | Rows per Arrow batch (default: 100,000) |
| `extensions` | no | config | DuckDB extensions to install/load (default: `["iceberg", "aws", "httpfs"]`) |

**Validation:** At least one of `namespace` or `table_names` must be provided. If both are provided, `table_names` takes precedence.

### Resource: `glue_iceberg_table()`

A standalone `@dlt.resource` for loading a single Glue Iceberg table (mirrors the `sql_database` / `sql_table` pattern).

### Configuration via files

```toml
# config.toml
[sources.glue_iceberg]
aws_account_id = "123456789012"
region = "us-east-2"
namespace = "my_database"
extensions = ["iceberg", "aws", "httpfs"]

# secrets.toml
[sources.glue_iceberg]
role_arn = "arn:aws:iam::123456789012:role/my-role"
```

## Table Discovery & Resource Generation

1. Creates a temporary in-memory DuckDB connection.
2. Calls `WithTableScanners._setup_iceberg()` for Iceberg extension install/load with version handling.
3. Loads additional configured extensions via the `DuckDbConnectionPool` pattern.
4. Calls `DuckDbSqlClient.create_secret()` for S3 credentials (reusing existing credential chain / explicit key logic).
5. `ATTACH`es to the Glue catalog:
   ```sql
   ATTACH 'account_id' AS glue_catalog (
       TYPE iceberg,
       ENDPOINT_TYPE 'glue'
   );
   ```
6. Discovers tables:
   - If `table_names` provided: use those directly.
   - If `namespace` provided: `SHOW TABLES IN glue_catalog.{namespace}`.
7. For each table, yields a `dlt.resource` that fetches Arrow batches.

### Resource per table

```python
@dlt.resource(name="orders", write_disposition="replace")
def _table_resource(
    incremental: Optional[dlt.sources.incremental] = None,
) -> Iterator[pa.Table]:
    query = f"SELECT * FROM glue_catalog.{namespace}.{table}"
    if incremental and incremental.cursor_path and incremental.last_value is not None:
        query += f" WHERE {incremental.cursor_path} >= $1"
        params = [incremental.last_value]

    # Reuses DuckDBDBApiCursorImpl.iter_arrow() for chunked reads
    result = conn.execute(query)
    for batch in result.iter_arrow(chunk_size):
        yield batch
```

### Incremental loading (opt-in)

Default write disposition is `replace` (full table). Users opt into incremental per table using standard dlt patterns:

```python
source = glue_iceberg(
    aws_account_id="123456789012",
    region="us-east-2",
    namespace="my_db",
)
source.orders.apply_hints(
    incremental=dlt.sources.incremental("updated_at")
)
pipeline.run(source)
```

## AWS Authentication

Reuses `DuckDbSqlClient.create_secret()` which already supports:

1. **Explicit credentials** (if `aws_access_key_id` and `aws_secret_access_key` provided):
   ```sql
   CREATE SECRET (
       TYPE s3,
       KEY_ID 'AKIA...',
       SECRET 'wJal...',
       REGION 'us-east-2'
   );
   ```

2. **Credential chain** (default fallback — env vars, `~/.aws/credentials`, instance profile):
   ```sql
   CREATE SECRET (
       TYPE s3,
       PROVIDER credential_chain,
       REGION 'us-east-2'
   );
   ```

3. **STS assume-role** (if `role_arn` provided):
   ```sql
   CREATE SECRET (
       TYPE s3,
       PROVIDER credential_chain,
       CHAIN sts,
       ASSUME_ROLE_ARN 'arn:aws:iam::...:role/...',
       REGION 'us-east-2'
   );
   ```

## Connection Lifecycle

- One shared in-memory DuckDB connection per source invocation.
- Created during source initialization (before resources are iterated).
- All resources read from the same attached Glue catalog.
- Connection is closed when the source generator is exhausted or on error.

```python
@dlt.source
def glue_iceberg(...) -> Iterable[DltResource]:
    conn = duckdb.connect(":memory:")
    try:
        WithTableScanners._setup_iceberg(conn)
        _load_extensions(conn, extensions)
        _create_secret(conn, ...)  # delegates to DuckDbSqlClient.create_secret()
        _attach_catalog(conn, aws_account_id, region)
        tables = _discover_tables(conn, namespace, table_names)
        for table in tables:
            yield _make_resource(conn, table, chunk_size)
    except Exception:
        conn.close()
        raise
```

## Error Handling

Minimal — DuckDB's errors are already descriptive:

- **Bad credentials / unreachable Glue** — DuckDB raises on `ATTACH`; propagate with context.
- **Table not found** — DuckDB raises on `SELECT`; propagate as-is.
- **Network issues mid-read** — DuckDB handles S3 retries internally; if it fails, the resource fails and dlt marks it.

No custom exception hierarchy. Connection cleanup is the only concern.

## File Structure

Extends the existing DuckDB codebase with a thin Glue-specific source layer:

```
dlt/sources/glue_iceberg/
├── __init__.py          # Public API: glue_iceberg(), glue_iceberg_table()
├── helpers.py           # Glue-specific logic: catalog attach, table discovery
│                        # Imports and delegates to existing DuckDB code:
│                        #   - WithTableScanners._setup_iceberg()
│                        #   - DuckDbSqlClient.create_secret()
│                        #   - DuckDBDBApiCursorImpl for Arrow iteration
├── settings.py          # Defaults (DEFAULT_EXTENSIONS, DEFAULT_CHUNK_SIZE)

tests/sources/glue_iceberg/
├── test_glue_iceberg_source.py   # Unit tests (mocked DuckDB)
├── test_helpers.py               # Helper function tests
```

**New code is minimal** — `helpers.py` orchestrates existing DuckDB infrastructure with three Glue-specific additions:
- `attach_glue_catalog()` — the `ATTACH ... TYPE iceberg, ENDPOINT_TYPE 'glue'` statement
- `discover_tables()` — `SHOW TABLES` query against the attached catalog
- `make_resource()` — wires up `iter_arrow()` with incremental filtering

## Dependencies

No new dependencies. Uses what `dlt[duckdb]` already provides:
- **`duckdb`** — optional extra in dlt. Imported lazily with `MissingDependencyException` if not installed.
- **`pyarrow`** — transitive dependency of `duckdb`.

## Defaults (settings.py)

```python
DEFAULT_EXTENSIONS = ["iceberg", "aws", "httpfs"]
DEFAULT_CHUNK_SIZE = 100_000
```

## Usage Example

```python
import dlt
from dlt.sources.glue_iceberg import glue_iceberg

# Load all tables from a Glue database
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
```

## References

- [DuckDB Iceberg + AWS Glue](https://duckdb.org/docs/lts/core_extensions/iceberg/amazon_sagemaker_lakehouse) — DuckDB's native Glue catalog support (experimental)
- [dlt DuckDB destination](https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb) — existing dlt DuckDB configuration and usage
- `dlt/destinations/impl/duckdb/sql_client.py` — existing DuckDB SQL client with secrets, Iceberg, and table scanner support
