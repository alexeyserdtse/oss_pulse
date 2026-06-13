import json
import logging
import uuid
from typing import Any

import pytest
import responses as responses_lib

from github_ingest.__main__ import _JsonFormatter
from github_ingest.client import GitHubClient
from github_ingest.common import EntitySpec
from github_ingest.config import Settings
from github_ingest.extractors import Extractor
from github_ingest.models import _VALIDATION_MODELS
from github_ingest.pipeline import IngestionPipeline
from github_ingest.warehouse import DuckDBConnector

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


def test_validation_models_importable_from_models() -> None:
    assert "repo" in _VALIDATION_MODELS
    assert "issue" in _VALIDATION_MODELS
    assert "commit" in _VALIDATION_MODELS
    assert "contributor" in _VALIDATION_MODELS


def test_json_formatter_emits_parseable_json() -> None:
    formatter = _JsonFormatter()
    record = logging.LogRecord(
        name="github_ingest.pipeline",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Bronze write complete",
        args=(),
        exc_info=None,
    )
    record.__dict__["load_id"] = "load-abc"
    record.__dict__["entity"] = "repo"

    output = formatter.format(record)
    parsed = json.loads(output)

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "github_ingest.pipeline"
    assert parsed["message"] == "Bronze write complete"
    assert parsed["load_id"] == "load-abc"
    assert parsed["entity"] == "repo"
    assert "timestamp" in parsed


def test_json_formatter_standard_fields_only_no_logging_internals() -> None:
    formatter = _JsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname="",
        lineno=0,
        msg="drift",
        args=(),
        exc_info=None,
    )
    output = formatter.format(record)
    parsed = json.loads(output)
    assert "message" in parsed
    assert "lineno" not in parsed
    assert "pathname" not in parsed


@responses_lib.activate
def test_request_debug_fires_with_endpoint(caplog: pytest.LogCaptureFixture) -> None:
    settings = Settings(github_token=None, github_base_url="https://api.github.com", page_size=100)
    client = GitHubClient(settings)

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json={"id": 84823001, "full_name": "duckdb/duckdb"},
        status=200,
    )

    with caplog.at_level(logging.DEBUG, logger="github_ingest.client"):
        client.get("/repos/duckdb/duckdb")

    debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert any(getattr(r, "endpoint", None) == "/repos/duckdb/duckdb" for r in debug_records)
    client.close()


@responses_lib.activate
def test_bronze_write_info_fires_with_context(
    issue_fixture: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    _ISSUE_SPEC_LOG = EntitySpec(
        name="issue",
        table_name="bronze.issues",
        endpoint="/repos/{owner}/{name}/issues",
        key_field="id",
        single_object=False,
        supports_since=True,
        paginate=True,
        discriminator="issue_or_pr",
    )

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        json=[issue_fixture],
        status=200,
    )

    settings = Settings(github_token=None, github_base_url="https://api.github.com", page_size=100)
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_ISSUE_SPEC_LOG, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)
        with caplog.at_level(logging.INFO, logger="github_ingest.pipeline"):
            pipeline.run(["duckdb/duckdb"])

    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    bronze_records = [r for r in info_records if "Bronze write" in r.message]
    assert bronze_records, "expected Bronze write INFO log"
    rec = bronze_records[0]
    assert getattr(rec, "load_id", None) is not None
    assert getattr(rec, "entity", None) == "issue"
    assert getattr(rec, "table", None) == "bronze.issues"
    assert getattr(rec, "row_count", None) == 1
    client.close()


@responses_lib.activate
def test_304_skip_info_fires_for_paginated_entity(
    issue_fixture: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    _ISSUE_SPEC_LOG = EntitySpec(
        name="issue",
        table_name="bronze.issues",
        endpoint="/repos/{owner}/{name}/issues",
        key_field="id",
        single_object=False,
        supports_since=True,
        paginate=True,
        discriminator="issue_or_pr",
    )

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        json=[issue_fixture],
        status=200,
        headers={"ETag": '"etag-log-test"'},
    )
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        status=304,
    )

    settings = Settings(github_token=None, github_base_url="https://api.github.com", page_size=100)
    client = GitHubClient(settings)

    with DuckDBConnector(":memory:") as wh:
        extractors = [Extractor(_ISSUE_SPEC_LOG, client)]
        pipeline = IngestionPipeline(settings, client, wh, extractors)
        pipeline.run(["duckdb/duckdb"])
        with caplog.at_level(logging.INFO, logger="github_ingest.pipeline"):
            pipeline.run(["duckdb/duckdb"])

    skip_records = [r for r in caplog.records if "304 skip" in r.message]
    assert skip_records, "expected 304 skip INFO log"
    rec = skip_records[0]
    assert getattr(rec, "load_id", None) is not None
    assert getattr(rec, "entity", None) == "issue"
    client.close()


@responses_lib.activate
def test_token_never_appears_in_log_output(
    repo_fixture: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    secret_token = f"ghp_{uuid.uuid4().hex}"

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json=repo_fixture,
        status=200,
    )

    settings = Settings(
        github_token=secret_token,  # type: ignore[arg-type]
        github_base_url="https://api.github.com",
        page_size=100,
    )

    with caplog.at_level(logging.DEBUG):
        client = GitHubClient(settings)
        with DuckDBConnector(":memory:") as wh:
            extractors = [Extractor(_REPO_SPEC, client)]
            pipeline = IngestionPipeline(settings, client, wh, extractors)
            pipeline.run(["duckdb/duckdb"])
        client.close()

    full_log = caplog.text
    assert secret_token not in full_log, "Token leaked into log output"
