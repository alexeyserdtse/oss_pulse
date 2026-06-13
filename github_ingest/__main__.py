import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from github_ingest.client import GitHubClient
from github_ingest.common import load_entities, load_repos
from github_ingest.config import Settings
from github_ingest.extractors import Extractor
from github_ingest.pipeline import IngestionPipeline
from github_ingest.warehouse import DuckDBConnector

_COMMON_INGEST_DIR = Path(__file__).parent.parent / "common" / "ingest"
_ALL_ENTITIES = ["repo", "issue", "commit", "contributor"]


_LOG_RECORD_BUILTIN_KEYS = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys() | {"message", "asctime"}
)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _LOG_RECORD_BUILTIN_KEYS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)


logger = logging.getLogger(__name__)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(prog="github_ingest")
    parser.add_argument("--repos", nargs="+", metavar="OWNER/REPO")
    parser.add_argument("--entities", nargs="+", choices=_ALL_ENTITIES, default=_ALL_ENTITIES)
    parser.add_argument(
        "--mode",
        choices=["daily", "history"],
        default="daily",
        help="daily: conditional requests using stored state; history: full pull, append snapshot",
    )
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args()

    settings = Settings()
    log_level = args.log_level if args.log_level is not None else settings.log_level
    _configure_logging(log_level)

    full_refresh = args.mode == "history"
    run_id = str(uuid.uuid4())

    all_specs = {spec.name: spec for spec in load_entities(_COMMON_INGEST_DIR / "entities.json")}
    repos = args.repos if args.repos else load_repos(_COMMON_INGEST_DIR / "repos.json")

    client = GitHubClient(settings)
    try:
        with DuckDBConnector(str(settings.duckdb_path)) as wh:
            extractors = [Extractor(all_specs[e], client) for e in args.entities]
            pipeline = IngestionPipeline(settings, client, wh, extractors)
            summary = pipeline.run(repos, full_refresh=full_refresh)

        logger.info(
            "Run complete",
            extra={
                "run_id": run_id,
                "mode": args.mode,
                "rows_by_table": summary.rows_by_table,
                "validation_failures": summary.validation_failures,
                "skipped_304": summary.skipped_304,
            },
        )
    except Exception:
        logger.exception("Infra failure during ingestion", extra={"run_id": run_id})
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
