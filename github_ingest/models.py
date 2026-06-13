from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RepoModel(BaseModel):
    id: int
    full_name: str
    description: str | None = None
    stargazers_count: int = 0
    forks_count: int = 0
    open_issues_count: int = 0
    language: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    pushed_at: datetime | None = None
    default_branch: str = "main"
    topics: list[str] = Field(default_factory=list)
    watchers_count: int = 0
    subscribers_count: int = 0
    size: int = 0
    archived: bool = False
    disabled: bool = False


class IssueModel(BaseModel):
    id: int
    number: int
    title: str
    state: str
    user: dict[str, Any] | None = None
    labels: list[dict[str, Any]] = Field(default_factory=list)
    assignees: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None
    body: str | None = None
    comments: int = 0
    pull_request: dict[str, Any] | None = None


class CommitModel(BaseModel):
    sha: str
    commit: dict[str, Any]
    author: dict[str, Any] | None = None
    committer: dict[str, Any] | None = None
    parents: list[dict[str, Any]] = Field(default_factory=list)


class ContributorModel(BaseModel):
    id: int
    login: str
    type: str = "User"
    contributions: int = 0
    avatar_url: str | None = None
    html_url: str | None = None
    site_admin: bool = False


_VALIDATION_MODELS: dict[str, type[BaseModel]] = {
    "repo": RepoModel,
    "issue": IssueModel,
    "commit": CommitModel,
    "contributor": ContributorModel,
}
