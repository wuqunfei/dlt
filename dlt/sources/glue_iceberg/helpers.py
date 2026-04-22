from typing import TYPE_CHECKING, Any, Iterator, List, Optional

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException

from dlt.sources.glue_iceberg.settings import (
    DEFAULT_EXTENSIONS,
    DEFAULT_CHUNK_SIZE,
    GLUE_CATALOG_ALIAS,
)

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa
else:
    try:
        import duckdb
    except ImportError:
        duckdb = None


def _ensure_duckdb() -> None:
    if duckdb is None:
        raise MissingDependencyException("glue_iceberg source", ["duckdb"])


def _install_extensions(
    conn: "duckdb.DuckDBPyConnection",
    extensions: List[str],
) -> None:
    """Install and load DuckDB extensions."""
    for ext in extensions:
        conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")


def _create_aws_secret(
    conn: "duckdb.DuckDBPyConnection",
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
    conn: "duckdb.DuckDBPyConnection",
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
) -> "duckdb.DuckDBPyConnection":
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
    _ensure_duckdb()
    exts = extensions or DEFAULT_EXTENSIONS
    conn = duckdb.connect(":memory:")
    _install_extensions(conn, exts)
    _create_aws_secret(conn, region, aws_access_key_id, aws_secret_access_key, role_arn)
    _attach_glue_catalog(conn, aws_account_id, region)
    return conn


def discover_tables(
    conn: "duckdb.DuckDBPyConnection",
    namespace: str,
) -> List[str]:
    """Discover all table names in a Glue catalog namespace.

    Args:
        conn: DuckDB connection with attached Glue catalog.
        namespace: Glue database name.

    Returns:
        List of table names.
    """
    result = conn.execute(f"SHOW TABLES IN {GLUE_CATALOG_ALIAS}.{namespace}").fetchall()
    return [row[0] for row in result]


def fetch_arrow_batches(
    conn: "duckdb.DuckDBPyConnection",
    namespace: str,
    table: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    incremental_column: Optional[str] = None,
    last_value: Optional[Any] = None,
) -> Iterator["pa.Table"]:
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
    params: List[Any] = []
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
