from typing import Any

from github_ingest.client import GitHubClient
from github_ingest.common import EntitySpec
from github_ingest.config import Settings
from github_ingest.extractors import Extractor

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

_COMMIT_SPEC = EntitySpec(
    name="commit",
    table_name="bronze.commits",
    endpoint="/repos/{owner}/{name}/commits",
    key_field="sha",
    single_object=False,
    supports_since=True,
    paginate=True,
    discriminator="default",
)

_CONTRIBUTOR_SPEC = EntitySpec(
    name="contributor",
    table_name="bronze.contributors",
    endpoint="/repos/{owner}/{name}/contributors",
    key_field="id",
    single_object=False,
    supports_since=False,
    paginate=True,
    discriminator="default",
)


def make_client() -> GitHubClient:
    settings = Settings(github_token=None, github_base_url="https://api.github.com")
    return GitHubClient(settings)


def test_repo_extractor_record_id(repo_fixture: dict[str, Any]) -> None:
    client = make_client()
    ex = Extractor(_REPO_SPEC, client)
    assert ex.record_id(repo_fixture) == "84823001"
    assert ex.discriminator(repo_fixture) == "repo"
    assert ex.endpoint("duckdb/duckdb") == "/repos/duckdb/duckdb"
    client.close()


def test_issue_extractor_discriminator_issue(issue_fixture: dict[str, Any]) -> None:
    client = make_client()
    ex = Extractor(_ISSUE_SPEC, client)
    assert ex.discriminator(issue_fixture) == "issue"
    assert ex.record_id(issue_fixture) == "2100001"
    client.close()


def test_issue_extractor_discriminator_pr(pr_fixture: dict[str, Any]) -> None:
    client = make_client()
    ex = Extractor(_ISSUE_SPEC, client)
    assert ex.discriminator(pr_fixture) == "pull_request"
    assert ex.record_id(pr_fixture) == "2100002"
    client.close()


def test_commit_extractor_record_id(commit_fixture: dict[str, Any]) -> None:
    client = make_client()
    ex = Extractor(_COMMIT_SPEC, client)
    assert ex.record_id(commit_fixture) == "abc123def456abc123def456abc123def456abc1"
    assert ex.endpoint("duckdb/duckdb") == "/repos/duckdb/duckdb/commits"
    client.close()


def test_contributor_extractor_record_id(contributor_fixture: dict[str, Any]) -> None:
    client = make_client()
    ex = Extractor(_CONTRIBUTOR_SPEC, client)
    assert ex.record_id(contributor_fixture) == "5001"
    assert ex.endpoint("duckdb/duckdb") == "/repos/duckdb/duckdb/contributors"
    client.close()
