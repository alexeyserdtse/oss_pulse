import hashlib
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from github_ingest.client import GitHubClient
from github_ingest.config import Settings
from github_ingest.extractors import Extractor
from github_ingest.warehouse import DuckDBConnector

logger = logging.getLogger(__name__)


@dataclass
class RunSummary:
    rows_by_table: dict[str, int] = field(default_factory=dict)
    validation_failures: dict[str, int] = field(default_factory=dict)
    skipped_304: int = 0


def _params_hash(params: dict[str, Any] | None) -> str:
    serialized = json.dumps(params or {}, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _max_since(current: str | None, candidate: str | None) -> str | None:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return candidate if candidate > current else current


class IngestionPipeline:
    def __init__(
        self,
        settings: Settings,
        client: GitHubClient,
        warehouse: DuckDBConnector,
        extractors: Sequence[Extractor],
    ) -> None:
        self._settings = settings
        self._client = client
        self._warehouse = warehouse
        self._extractors: Sequence[Extractor] = extractors

    def run(self, repos: list[str], full_refresh: bool = False) -> RunSummary:
        summary = RunSummary()

        for repo in repos:
            for extractor in self._extractors:
                self._run_extractor(repo, extractor, full_refresh, summary)

        return summary

    def _run_extractor(
        self,
        repo: str,
        extractor: Extractor,
        full_refresh: bool,
        summary: RunSummary,
    ) -> None:
        endpoint = extractor.endpoint(repo)
        base_params: dict[str, Any] = {"per_page": self._settings.page_size}

        state = self._warehouse.get_state(endpoint, _params_hash(base_params))
        etag = None if full_refresh else (state or {}).get("etag")
        last_modified = None if full_refresh else (state or {}).get("last_modified")
        since = None if full_refresh else (state or {}).get("last_since")

        params = dict(base_params)
        if since is not None and extractor.spec.supports_since:
            params["since"] = since

        load_id = DuckDBConnector.new_load_id()
        self._warehouse.ensure_bronze_table(extractor.table_name)

        log_ctx: dict[str, Any] = {
            "load_id": load_id,
            "repo": repo,
            "entity": extractor.entity,
            "endpoint": endpoint,
        }

        rows: list[dict[str, Any]] = []
        ingested_at = datetime.now(UTC)
        last_status = 200
        max_since: str | None = None

        if extractor.spec.single_object:
            resp = self._client.get(endpoint, etag=etag, last_modified=last_modified)
            last_status = resp.status_code
            if resp.status_code == 304:
                logger.info("304 skip", extra={**log_ctx, "status": 304})
                summary.skipped_304 += 1
                self._warehouse.upsert_state(
                    endpoint,
                    _params_hash(base_params),
                    etag,
                    last_modified,
                    since,
                    load_id,
                    304,
                )
                return
            raw = resp.json()
            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")
            rows.append(self._build_row(extractor, raw, repo, load_id, ingested_at, summary))
            max_since = _max_since(max_since, extractor.spec.extract_since_value(raw))
        else:
            paged = extractor.paginate_raw(
                repo,
                etag=etag,
                last_modified=last_modified,
                params=params,
            )

            if paged.was_304:
                logger.info("304 skip", extra={**log_ctx, "status": 304})
                summary.skipped_304 += 1
                self._warehouse.upsert_state(
                    endpoint,
                    _params_hash(base_params),
                    etag,
                    last_modified,
                    since,
                    load_id,
                    304,
                )
                return

            etag = paged.etag
            last_modified = paged.last_modified

            for raw in paged:
                rows.append(self._build_row(extractor, raw, repo, load_id, ingested_at, summary))
                max_since = _max_since(max_since, extractor.spec.extract_since_value(raw))

            last_status = 200

        if rows:
            self._warehouse.append(extractor.table_name, rows)
            summary.rows_by_table[extractor.table_name] = summary.rows_by_table.get(
                extractor.table_name, 0
            ) + len(rows)
            logger.info(
                "Bronze write complete",
                extra={**log_ctx, "table": extractor.table_name, "row_count": len(rows)},
            )

        stored_since = max_since if max_since is not None else since
        self._warehouse.upsert_state(
            endpoint,
            _params_hash(base_params),
            etag,
            last_modified,
            stored_since,
            load_id,
            last_status,
        )

    def _build_row(
        self,
        extractor: Extractor,
        raw: dict[str, Any],
        repo: str,
        load_id: str,
        ingested_at: datetime,
        summary: RunSummary,
    ) -> dict[str, Any]:
        raw_id = extractor.record_id(raw)
        if raw_id is None:
            payload_hash = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()[:16]
            record_id = f"nokey:{payload_hash}"
            entity = extractor.discriminator(raw)
            summary.validation_failures[extractor.table_name] = (
                summary.validation_failures.get(extractor.table_name, 0) + 1
            )
            logger.warning(
                "Missing natural key",
                extra={
                    "load_id": load_id,
                    "entity": entity,
                    "record_id": record_id,
                    "repo": repo,
                },
            )
        else:
            record_id = raw_id
            entity = extractor.discriminator(raw)

        try:
            extractor.validate(raw)
        except ValidationError as exc:
            table = extractor.table_name
            summary.validation_failures[table] = summary.validation_failures.get(table, 0) + 1
            logger.warning(
                "Validation drift",
                extra={
                    "load_id": load_id,
                    "entity": entity,
                    "record_id": record_id,
                    "repo": repo,
                    "error_count": exc.error_count(),
                },
            )

        return {
            "load_id": load_id,
            "record_id": record_id,
            "entity": entity,
            "source_repo": repo,
            "ingested_at": ingested_at,
            "payload": json.dumps(raw),
        }
