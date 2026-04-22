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
        raise ValueError("At least one of `namespace` or `table_names` must be provided.")

    conn = create_glue_connection(
        aws_account_id=aws_account_id,
        region=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        role_arn=role_arn,
        extensions=extensions,
    )

    if table_names:
        parsed = []
        for t in table_names:
            parts = t.rsplit(".", 1)
            if len(parts) == 2:
                parsed.append((parts[0], parts[1]))
            else:
                if not namespace:
                    raise ValueError(
                        f"Table name '{t}' has no namespace prefix and `namespace` is not set."
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
