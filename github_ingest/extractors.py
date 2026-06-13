from typing import Any

from pydantic import BaseModel

from github_ingest.client import GitHubClient, PagedResult
from github_ingest.common import EntitySpec
from github_ingest.models import _VALIDATION_MODELS


class Extractor:
    def __init__(self, spec: EntitySpec, client: GitHubClient) -> None:
        self._spec = spec
        self._client = client

    @property
    def entity(self) -> str:
        return self._spec.name

    @property
    def table_name(self) -> str:
        return self._spec.table_name

    @property
    def spec(self) -> EntitySpec:
        return self._spec

    def endpoint(self, repo: str) -> str:
        owner, name = repo.split("/", 1)
        return self._spec.endpoint.format(owner=owner, name=name)

    def record_id(self, raw: dict[str, Any]) -> str | None:
        v = raw.get(self._spec.key_field)
        return str(v) if v is not None else None

    def discriminator(self, raw: dict[str, Any]) -> str:
        if self._spec.discriminator == "issue_or_pr":
            return "pull_request" if raw.get("pull_request") is not None else self._spec.name
        return self._spec.name

    def validate(self, raw: dict[str, Any]) -> BaseModel | None:
        model_cls = _VALIDATION_MODELS.get(self._spec.name)
        if model_cls is None:
            return None
        return model_cls.model_validate(raw)

    def paginate_raw(
        self,
        repo: str,
        etag: str | None = None,
        last_modified: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> PagedResult:
        return self._client.paginate(
            self.endpoint(repo),
            params=params,
            etag=etag,
            last_modified=last_modified,
        )
