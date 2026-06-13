import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from github_ingest.warehouse import DuckDBConnector

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES_DIR / f"{name}.json").read_text())
    return data


@pytest.fixture
def repo_fixture() -> dict[str, Any]:
    return load_fixture("repo")


@pytest.fixture
def issue_fixture() -> dict[str, Any]:
    return load_fixture("issue")


@pytest.fixture
def pr_fixture() -> dict[str, Any]:
    return load_fixture("pull_request")


@pytest.fixture
def commit_fixture() -> dict[str, Any]:
    return load_fixture("commit")


@pytest.fixture
def contributor_fixture() -> dict[str, Any]:
    return load_fixture("contributor")


@pytest.fixture
def memory_wh() -> Generator[DuckDBConnector, None, None]:
    with DuckDBConnector(":memory:") as wh:
        yield wh
