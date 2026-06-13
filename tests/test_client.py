import logging
import time
from unittest.mock import patch

import pytest
import responses as responses_lib

from github_ingest.client import GitHubClient, _parse_next_link
from github_ingest.config import Settings


def make_settings(max_retries: int = 2) -> Settings:
    return Settings(
        github_token=None,
        github_base_url="https://api.github.com",
        page_size=2,
        request_timeout=5,
        max_retries=max_retries,
    )


@responses_lib.activate
def test_paginate_follows_link_rel_next() -> None:
    settings = make_settings()
    client = GitHubClient(settings)

    page1 = [{"id": 1}, {"id": 2}]
    page2 = [{"id": 3}]

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        json=page1,
        headers={"Link": '<https://api.github.com/repos/duckdb/duckdb/issues?page=2>; rel="next"'},
        status=200,
    )
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues?page=2",
        json=page2,
        status=200,
    )

    results = list(client.paginate("/repos/duckdb/duckdb/issues"))
    assert len(results) == 3
    assert results[0]["id"] == 1
    assert results[2]["id"] == 3
    client.close()


@responses_lib.activate
def test_304_returned_not_raised() -> None:
    settings = make_settings()
    client = GitHubClient(settings)

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        status=304,
    )

    resp = client.get("/repos/duckdb/duckdb", etag='"abc123"')
    assert resp.status_code == 304
    client.close()


@responses_lib.activate
def test_5xx_retried() -> None:
    settings = make_settings(max_retries=3)
    client = GitHubClient(settings)

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        status=500,
    )
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json={"id": 1, "full_name": "duckdb/duckdb"},
        status=200,
    )

    resp = client.get("/repos/duckdb/duckdb")
    assert resp.status_code == 200
    client.close()


@responses_lib.activate
def test_rate_limit_sleep(caplog: pytest.LogCaptureFixture) -> None:
    settings = make_settings()
    client = GitHubClient(settings)

    future_reset = str(int(time.time()) + 2)
    expected_sleep = max(0, int(future_reset) - int(time.time())) + 1
    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb",
        json={"id": 1},
        headers={
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": future_reset,
        },
        status=200,
    )

    with (
        patch("github_ingest.client.time.sleep") as mock_sleep,
        caplog.at_level(logging.WARNING, logger="github_ingest.client"),
    ):
        resp = client.get("/repos/duckdb/duckdb")

    assert resp.status_code == 200
    mock_sleep.assert_called_once()
    actual_sleep = mock_sleep.call_args[0][0]
    assert actual_sleep >= 1
    assert actual_sleep <= expected_sleep + 2

    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("Rate limit" in r.message for r in warning_records)
    rate_limit_record = next(r for r in warning_records if "Rate limit" in r.message)
    assert rate_limit_record.sleep_seconds >= 1  # type: ignore[attr-defined]

    client.close()


@responses_lib.activate
def test_paginate_304_yields_nothing_no_crash() -> None:
    settings = make_settings()
    client = GitHubClient(settings)

    responses_lib.add(
        responses_lib.GET,
        "https://api.github.com/repos/duckdb/duckdb/issues",
        status=304,
    )

    results = list(client.paginate("/repos/duckdb/duckdb/issues", etag='"etag-abc"'))
    assert results == []
    client.close()


def test_parse_next_link_present() -> None:
    next_url = "https://api.github.com/repos/x/y/issues?page=2"
    last_url = "https://api.github.com/repos/x/y/issues?page=5"
    header = f'<{next_url}>; rel="next", <{last_url}>; rel="last"'
    assert _parse_next_link(header) == next_url


def test_parse_next_link_absent() -> None:
    header = '<https://api.github.com/repos/x/y/issues?page=5>; rel="last"'
    assert _parse_next_link(header) is None


def test_parse_next_link_empty() -> None:
    assert _parse_next_link("") is None
