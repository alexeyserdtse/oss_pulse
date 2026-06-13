from typing import Any

import responses as responses_lib

from github_ingest.client import GitHubClient
from github_ingest.common import EntitySpec
from github_ingest.config import Settings
from github_ingest.extractors import Extractor
from github_ingest.pipeline import IngestionPipeline
from github_ingest.warehouse import DuckDBConnector

_COMMIT_SPEC = EntitySpec(
    name="commit",
    table_name="bronze.commits",
    endpoint="/repos/{owner}/{name}/commits",
    key_field="sha",
    single_object=False,
    supports_since=True,
    since_field="commit.committer.date",
    paginate=True,
    discriminator="default",
)

_REPO_SPEC = EntitySpec(
    name="repo",
    table_name="bronze.repos",
    endpoint="/repos/{owner}/{name}",
    key_field="id",
    single_object=True,
    supports_since=False,
    paginate=False,
    discriminator="default",
)

_ISSUE_SPEC = EntitySpec(
    name="issue",
    table_name="bronze.issues",
    endpoint="/repos/{owner}/{name}/issues",
    key_field="id",
    single_object=False,
    supports_since=True,
    paginate=True,
    discriminator="issue_or_pr",
)


def make_settings() -> Settings:
    return Settings(github_token=None, github_base_url="https://api.github.com", page_size=100)


@responses_lib.activate
def test_pipeline_repo_counts_land(repo_fixture: dict[str, Any]) -> None:
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json=repo_fixture,
        status=200,
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_REPO_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)
        summary = pipeline.run(["duckdb/duckdb"])

    assert summary.rows_by_table.get("bronze.repos", 0) == 1
    assert summary.validation_failures == {}
    assert summary.skipped_304 == 0
    client.close()


@responses_lib.activate
def test_pipeline_304_skips_rows_and_increments(repo_fixture: dict[str, Any]) -> None:
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json=repo_fixture,
        status=200,
        headers={"ETag": '"etag1"'},
    )
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        status=304,
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_REPO_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)

        summary1 = pipeline.run(["duckdb/duckdb"])
        assert summary1.rows_by_table.get("bronze.repos", 0) == 1
        assert summary1.skipped_304 == 0

        summary2 = pipeline.run(["duckdb/duckdb"])
        assert summary2.rows_by_table.get("bronze.repos", 0) == 0
        assert summary2.skipped_304 == 1

        row = wh.conn.execute("SELECT COUNT(*) FROM bronze.repos").fetchone()
        assert row is not None and row[0] == 1

    client.close()


@responses_lib.activate
def test_pipeline_drift_still_persists_raw(issue_fixture: dict[str, Any]) -> None:
    drifted = dict(issue_fixture)
    drifted["created_at"] = "not-a-date"

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        json=[drifted],
        status=200,
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_ISSUE_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)
        summary = pipeline.run(["duckdb/duckdb"])

        assert summary.validation_failures.get("bronze.issues", 0) == 1
        row = wh.conn.execute("SELECT COUNT(*) FROM bronze.issues").fetchone()
        assert row is not None and row[0] == 1

    client.close()


@responses_lib.activate
def test_pipeline_multiple_issues_all_persisted(
    issue_fixture: dict[str, Any], pr_fixture: dict[str, Any]
) -> None:
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        json=[issue_fixture, pr_fixture],
        status=200,
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_ISSUE_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)
        summary = pipeline.run(["duckdb/duckdb"])

        assert summary.rows_by_table.get("bronze.issues", 0) == 2
        rows = wh.conn.execute("SELECT entity FROM bronze.issues ORDER BY entity").fetchall()
        entities = [r[0] for r in rows]
        assert "issue" in entities
        assert "pull_request" in entities

    client.close()


@responses_lib.activate
def test_pipeline_missing_natural_key_still_persists_raw(issue_fixture: dict[str, Any]) -> None:
    no_key = {k: v for k, v in issue_fixture.items() if k != "id"}

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        json=[no_key],
        status=200,
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_ISSUE_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)
        summary = pipeline.run(["duckdb/duckdb"])

        row = wh.conn.execute("SELECT record_id FROM bronze.issues").fetchone()
        assert row is not None
        assert row[0].startswith("nokey:")
        assert summary.rows_by_table.get("bronze.issues", 0) == 1
        assert summary.validation_failures.get("bronze.issues", 0) >= 1

    client.close()


@responses_lib.activate
def test_pipeline_history_mode_ignores_etag(repo_fixture: dict[str, Any]) -> None:
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json=repo_fixture,
        status=200,
        headers={"ETag": '"etag1"'},
    )
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json=repo_fixture,
        status=200,
        headers={"ETag": '"etag1"'},
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_REPO_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)

        pipeline.run(["duckdb/duckdb"])
        summary2 = pipeline.run(["duckdb/duckdb"], full_refresh=True)

        assert summary2.skipped_304 == 0
        assert summary2.rows_by_table.get("bronze.repos", 0) == 1

        row = wh.conn.execute("SELECT COUNT(*) FROM bronze.repos").fetchone()
        assert row is not None and row[0] == 2

    client.close()


@responses_lib.activate
def test_pipeline_paginated_304_second_run_writes_zero_rows(
    issue_fixture: dict[str, Any],
) -> None:
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        json=[issue_fixture],
        status=200,
        headers={"ETag": '"etag-issues-1"'},
    )
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        status=304,
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_ISSUE_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)

        summary1 = pipeline.run(["duckdb/duckdb"])
        assert summary1.rows_by_table.get("bronze.issues", 0) == 1
        assert summary1.skipped_304 == 0

        summary2 = pipeline.run(["duckdb/duckdb"])
        assert summary2.rows_by_table.get("bronze.issues", 0) == 0
        assert summary2.skipped_304 == 1

        row = wh.conn.execute("SELECT COUNT(*) FROM bronze.issues").fetchone()
        assert row is not None and row[0] == 1

        second_request = responses_lib.calls[1]
        assert second_request.request.headers.get("If-None-Match") == '"etag-issues-1"'

    client.close()


@responses_lib.activate
def test_watermark_stored_as_max_seen_not_wall_clock(
    commit_fixture: dict[str, Any],
) -> None:
    earlier = dict(commit_fixture)
    earlier["sha"] = "aaa000" + "0" * 34
    earlier["commit"] = dict(commit_fixture["commit"])
    earlier["commit"]["committer"] = {
        "name": "Alice",
        "email": "a@b.com",
        "date": "2024-01-05T00:00:00Z",
    }

    later = dict(commit_fixture)
    later["sha"] = "bbb111" + "1" * 34
    later["commit"] = dict(commit_fixture["commit"])
    later["commit"]["committer"] = {
        "name": "Bob",
        "email": "b@b.com",
        "date": "2024-03-20T12:00:00Z",
    }

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/commits",
        json=[earlier, later],
        status=200,
    )

    settings = make_settings()
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_COMMIT_SPEC, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)
        pipeline.run(["duckdb/duckdb"])

        endpoint = "/repos/duckdb/duckdb/commits"
        from github_ingest.pipeline import _params_hash

        params_hash = _params_hash({"per_page": settings.page_size, "since": None})
        state = wh.get_state(endpoint, params_hash)
        if state is None:
            params_hash_base = _params_hash({"per_page": settings.page_size})
            state = wh.get_state(endpoint, params_hash_base)
        assert state is not None
        assert state["last_since"] == "2024-03-20T12:00:00Z"

    client.close()
