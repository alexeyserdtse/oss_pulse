import uuid
from datetime import UTC, datetime
from typing import Any

import duckdb
import pyarrow as pa

_BRONZE_SCHEMA = "bronze"

_BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    load_id VARCHAR,
    record_id VARCHAR,
    entity VARCHAR,
    source_repo VARCHAR,
    ingested_at TIMESTAMPTZ,
    payload JSON
)
"""

_STATE_DDL = """
CREATE TABLE IF NOT EXISTS bronze.ingestion_state (
    endpoint VARCHAR,
    params_hash VARCHAR,
    etag VARCHAR,
    last_modified VARCHAR,
    last_since VARCHAR,
    last_load_id VARCHAR,
    last_status INTEGER,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (endpoint, params_hash)
)
"""

_STATE_KEYS = ["etag", "last_modified", "last_since", "last_load_id", "last_status", "updated_at"]

_UPSERT_STATE = """
    INSERT INTO bronze.ingestion_state
        (endpoint, params_hash, etag, last_modified, last_since,
         last_load_id, last_status, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (endpoint, params_hash) DO UPDATE SET
        etag = excluded.etag,
        last_modified = excluded.last_modified,
        last_since = excluded.last_since,
        last_load_id = excluded.last_load_id,
        last_status = excluded.last_status,
        updated_at = excluded.updated_at
"""


class DuckDBConnector:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> "DuckDBConnector":
        self._conn = duckdb.connect(self._db_path)
        self.ensure_schema()
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        assert self._conn is not None, "DuckDBConnector used outside context manager"
        return self._conn

    def ensure_schema(self) -> None:
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_BRONZE_SCHEMA}")
        self.conn.execute(_STATE_DDL)

    def ensure_bronze_table(self, table_name: str) -> None:
        self.conn.execute(_BRONZE_DDL.format(table=table_name))

    def append(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        schema = pa.schema(
            [
                pa.field("load_id", pa.string()),
                pa.field("record_id", pa.string()),
                pa.field("entity", pa.string()),
                pa.field("source_repo", pa.string()),
                pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
                pa.field("payload", pa.string()),
            ]
        )
        arrow_batch = pa.Table.from_pylist(rows, schema=schema)
        self.conn.register("_arrow_batch", arrow_batch)
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM _arrow_batch")
        self.conn.unregister("_arrow_batch")

    def get_state(self, endpoint: str, params_hash: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT etag, last_modified, last_since, last_load_id, last_status, updated_at "
            "FROM bronze.ingestion_state WHERE endpoint = ? AND params_hash = ?",
            [endpoint, params_hash],
        ).fetchone()
        if row is None:
            return None
        return dict(zip(_STATE_KEYS, row, strict=True))

    def upsert_state(
        self,
        endpoint: str,
        params_hash: str,
        etag: str | None,
        last_modified: str | None,
        last_since: str | None,
        last_load_id: str,
        last_status: int,
    ) -> None:
        now = datetime.now(UTC)
        self.conn.execute(
            _UPSERT_STATE,
            [
                endpoint,
                params_hash,
                etag,
                last_modified,
                last_since,
                last_load_id,
                last_status,
                now,
            ],
        )

    @staticmethod
    def new_load_id() -> str:
        return str(uuid.uuid4())
