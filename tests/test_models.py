from typing import Any

import pytest
from pydantic import ValidationError

from github_ingest.models import CommitModel, ContributorModel, IssueModel, RepoModel


def test_repo_parse_ok(repo_fixture: dict[str, Any]) -> None:
    repo = RepoModel.model_validate(repo_fixture)
    assert repo.id == 84823001
    assert repo.full_name == "duckdb/duckdb"
    assert repo.stargazers_count == 24000


def test_issue_parse_ok(issue_fixture: dict[str, Any]) -> None:
    issue = IssueModel.model_validate(issue_fixture)
    assert issue.id == 2100001
    assert issue.state == "open"
    assert issue.pull_request is None


def test_pr_parse_ok(pr_fixture: dict[str, Any]) -> None:
    pr = IssueModel.model_validate(pr_fixture)
    assert pr.pull_request is not None
    assert "url" in pr.pull_request


def test_commit_parse_ok(commit_fixture: dict[str, Any]) -> None:
    commit = CommitModel.model_validate(commit_fixture)
    assert commit.sha == "abc123def456abc123def456abc123def456abc1"
    assert commit.commit["message"] == "Fix window function bug"


def test_contributor_parse_ok(contributor_fixture: dict[str, Any]) -> None:
    contributor = ContributorModel.model_validate(contributor_fixture)
    assert contributor.login == "alice"
    assert contributor.contributions == 142


def test_repo_missing_required_field_raises() -> None:
    with pytest.raises(ValidationError):
        RepoModel.model_validate({"id": 1})


def test_issue_missing_created_at_raises() -> None:
    with pytest.raises(ValidationError):
        IssueModel.model_validate(
            {
                "id": 1,
                "number": 1,
                "title": "t",
                "state": "open",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        )
