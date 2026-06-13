from datetime import UTC, datetime

from github_ingest.warehouse import DuckDBConnector


def _count(wh: DuckDBConnector, table: str) -> int:
    row = wh.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    assert row is not None
    return int(row[0])


def test_ensure_schema_idempotent() -> None:
    with DuckDBConnector(":memory:") as wh:
        wh.ensure_schema()
        wh.ensure_schema()
        rows = wh.conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        schemas = [r[0] for r in rows]
        assert "bronze" in schemas


def test_ensure_bronze_table_idempotent() -> None:
    with DuckDBConnector(":memory:") as wh:
        wh.ensure_bronze_table("bronze.repos")
        wh.ensure_bronze_table("bronze.repos")
        assert _count(wh, "bronze.repos") == 0


def test_append_lands_rows() -> None:
    with DuckDBConnector(":memory:") as wh:
        wh.ensure_bronze_table("bronze.issues")
        rows = [
            {
                "load_id": "load1",
                "record_id": "1001",
                "entity": "issue",
                "source_repo": "duckdb/duckdb",
                "ingested_at": datetime.now(UTC),
                "payload": '{"id": 1001}',
            }
        ]
        wh.append("bronze.issues", rows)
        assert _count(wh, "bronze.issues") == 1


def test_append_is_additive() -> None:
    with DuckDBConnector(":memory:") as wh:
        wh.ensure_bronze_table("bronze.issues")
        row = {
            "load_id": "load1",
            "record_id": "1001",
            "entity": "issue",
            "source_repo": "duckdb/duckdb",
            "ingested_at": datetime.now(UTC),
            "payload": '{"id": 1001}',
        }
        wh.append("bronze.issues", [row])
        wh.append("bronze.issues", [row])
        assert _count(wh, "bronze.issues") == 2


def test_state_round_trip() -> None:
    with DuckDBConnector(":memory:") as wh:
        wh.upsert_state(
            endpoint="/repos/duckdb/duckdb",
            params_hash="abc123",
            etag='"etag1"',
            last_modified="Sat, 01 Jan 2024 00:00:00 GMT",
            last_since="2024-01-01T00:00:00Z",
            last_load_id="load1",
            last_status=200,
        )
        state = wh.get_state("/repos/duckdb/duckdb", "abc123")
        assert state is not None
        assert state["etag"] == '"etag1"'
        assert state["last_load_id"] == "load1"
        assert state["last_status"] == 200


def test_state_upsert_updates_existing() -> None:
    with DuckDBConnector(":memory:") as wh:
        wh.upsert_state("/ep", "hash1", '"etag1"', None, None, "load1", 200)
        wh.upsert_state("/ep", "hash1", '"etag2"', None, None, "load2", 200)
        state = wh.get_state("/ep", "hash1")
        assert state is not None
        assert state["etag"] == '"etag2"'
        assert state["last_load_id"] == "load2"


def test_state_missing_returns_none() -> None:
    with DuckDBConnector(":memory:") as wh:
        assert wh.get_state("/nonexistent", "hash") is None


def test_new_load_id_unique() -> None:
    ids = {DuckDBConnector.new_load_id() for _ in range(100)}
    assert len(ids) == 100


def test_append_empty_rows_noop() -> None:
    with DuckDBConnector(":memory:") as wh:
        wh.ensure_bronze_table("bronze.commits")
        wh.append("bronze.commits", [])
        assert _count(wh, "bronze.commits") == 0
