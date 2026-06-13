# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

**Phase 2 complete — ingestion package built and tested.** The `github_ingest/` package, a 54-test
suite, and the CI `python` job are all live. `github-analytics-plan.md` is the authoritative spec;
this file is the operational summary.

Done: uv scaffold, `github_ingest/` (config, models, client, extractors, warehouse, pipeline,
CLI), 54 tests (real in-memory DuckDB; HTTP mocked at the boundary), CI running ruff + mypy + pytest.

Not yet built: dbt layer (bronze→silver→gold), Airflow DAG, Evidence dashboards.

Thesis: **stars measure hype, not health.** The project ranks a cohort of competing OSS data tools
by *dependency-risk* signals — bus factor, issue/PR responsiveness, liveness, contributor momentum —
to show which are safe to build on, and where health disagrees with popularity. Scope is the
maintainer/contributor side the GitHub API exposes: it measures project health, not adoption.

Goal: ingest OSS data-tooling repos from the GitHub REST API into a medallion DuckDB warehouse,
model with dbt, orchestrate with Airflow + Cosmos, visualize with Evidence.dev — all local, mostly
without Docker.

## Toolchain & commands

Python is managed with **uv**, pinned to **3.12** (dbt/Airflow don't yet support the system's
3.14). The commands below are exactly what CI runs (`.github/workflows/ci.yml`).

```bash
uv sync --all-extras          # install project + dev deps from uv.lock
uv run ruff check .           # lint
uv run ruff format --check .  # format check (drop --check to auto-format)
uv run mypy .                 # type-check
uv run pytest -q              # tests
```

dbt (once `dbt/` exists) runs in its **own venv** separate from Airflow's (dependency conflict):

```bash
dbt build      # run + test models against the configured DuckDB profile
dbt docs generate   # lineage graph (captured for README)
```

## Architecture (the parts that span files)

- **Medallion = dbt folders + DuckDB schemas.** Bronze (raw API payloads + `ingested_at`,
  `source_repo`) is written by the Python ingestion package and read by dbt as *sources* — dbt
  never builds bronze. Silver (`stg_`/`int_`) and gold (`dim_`/`fct_`/`mart_`) are dbt models.
  Three layers only; anything between silver and gold is a dbt **intermediate** model, never a
  new layer.

- **Bronze is fixed-schema, append-only raw JSON.** Every bronze table has the same six columns:
  `load_id`, `record_id`, `entity`, `source_repo`, `ingested_at`, `payload` (raw JSON verbatim).
  A separate `bronze.ingestion_state` table (keyed by `endpoint + params_hash`) stores ETag /
  Last-Modified / `since` values for conditional HTTP requests. `record_id` uses the natural key
  (repo id / issue id / commit sha / user id) or a deterministic `nokey:<hash>` fallback.

- **Config-as-data: tunable config lives in `common/*.json`; logic lives in classes.** `common/settings.json`
  holds non-secret scalars (`github_base_url`, `page_size`, `request_timeout`, `max_retries`,
  `duckdb_path`, `log_level`). `common/ingest/repos.json` is the ingestion source of truth for which
  repos to ingest. `common/ingest/entities.json` holds declarative `EntitySpec` records (`name`,
  `table_name`, `endpoint`, `key_field`, `single_object`, `supports_since`, `since_field`, `paginate`,
  `discriminator`). Pydantic validates all three files on load (`EntitySpec` model +
  `load_entities`/`load_repos` in `github_ingest/common.py`). **Secret boundary:** `github_token` is
  `SecretStr`, sourced from `ENV`/`.env` only — it has no JSON source and never appears in `common/`.
  A guardrail test enforces this.

- **Pydantic is the silver staging contract, not the bronze DDL.** `models.py` validates raw
  payloads non-blocking — raw is always persisted; validation failures are counted and logged, not
  rejected. Pydantic catches GitHub schema drift before it can silently corrupt silver transforms.
  The `issue_or_pr` discriminator strategy in `EntitySpec` assigns `entity = "pull_request"` when
  `pull_request` key is present, so issues and PRs share `bronze.issues` with a discriminator
  column. Entities built: `Repo`, `Issue` (+ PR discriminator), `Commit`, `Contributor`. Stargazer
  and a dedicated PR-detail extractor are deferred.

- **Single config-driven `Extractor(spec, client)` — no per-entity subclasses.** `Extractor` reads
  `EntitySpec` to resolve `endpoint()`, `record_id()` (via `spec.key_field`), `discriminator()`,
  and `validate()`. Two genuine logic branches remain in code: single-object fetch (`spec.single_object`)
  and the `issue_or_pr` discriminator. The `since` param is only sent to endpoints where
  `spec.supports_since=true`. All DuckDB-specific SQL is isolated in `warehouse.py` (swap-friendly).

- **Incremental ingestion via conditional requests + max-seen watermark.** The CLI accepts
  `--mode {daily,history}` (default `daily`). Daily mode threads ETag/Last-Modified headers and a
  `since` param through both single-object and paginated paths; a 304 response yields no rows and
  skips the write. The `since` watermark stored in `bronze.ingestion_state` is derived from the MAX
  value of each record's `since_field` (e.g. `updated_at` for issues, `commit.committer.date` for
  commits) — never from wall-clock time. History mode ignores stored state on read (full backfill)
  but remains append-only. Both modes are safe to re-run.

- **Structured JSON logging throughout.** `__main__.py` wires a zero-dependency JSON formatter
  (`_JsonFormatter`) to the root logger; all other modules use `logging.getLogger(__name__)` with
  `extra=` context (e.g. `load_id`, `repo`, `entity`, `mode`, `endpoint`). Key events: request
  DEBUG, 304-skip INFO, bronze-write INFO (table + row count), validation-drift/missing-key/rate-limit
  WARNING, run-summary INFO, infra ERROR. Tokens and payloads are never logged. Log level is set
  by `settings.json` `log_level` (default `"INFO"`), overridden at runtime by `--log-level`.

- **DuckDB is single-writer → the DAG is sequential.** `extract_github → dbt_snapshot →
  dbt_build → export_parquet`. There is never a concurrent writer. The final step exports gold
  marts to **parquet**, and Evidence reads the parquet — never the live `.duckdb` file — which is
  why there's no lock contention with the BI layer.

- **Airflow + Cosmos use two venvs and local execution mode.** `.venv-airflow` and `.venv-dbt`
  are separate to avoid the Airflow↔dbt dependency conflict; Cosmos runs in **local** mode with
  `ExecutionConfig.dbt_executable_path` pointing at the dbt venv, so each dbt model renders as its
  own Airflow task.

- **History needs accumulation.** Growth/trend metrics depend on the daily `snap_repo_stars`
  snapshot building up over time — the API can't supply them retroactively. Point-in-time metrics
  (response times, bus factor, throughput) work from day one.

## CI/CD & git workflow

- **`master` is protected:** force-push and deletion blocked; merges require the `lint-workflows`
  and `python` checks green with the branch up to date; a PR is required before merging (0
  approvals). Admins are not enforced, so the owner *can* direct-push (logged as a bypass) — but
  the normal path is **branch → PR → green checks → merge → delete branch**.
- **`ci.yml`** has `lint-workflows` (pinned, checksum-verified `actionlint`) and the `python` job
  above (ruff + mypy + pytest — all currently running and green). **`dependabot.yml`** opens weekly
  grouped PRs for github-actions and pip; they go through the same gate.
- **No release/deploy pipeline** — this is a data project, not a published artifact.
- Phase 8 will add a `dbt` CI job running `dbt build` against a committed fixture DuckDB so CI
  needs no GitHub API access.
