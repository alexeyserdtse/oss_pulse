# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

**Planning-complete, not yet scaffolded** — only CI/CD, the design document, and licensing exist.
`github-analytics-plan.md` is the authoritative spec (stack decisions, repo layout, OOP ingestion
design, dbt structure, gold marts, build phases). Read it before building any feature; this file
is the operational summary.

Goal: ingest OSS data-tooling repos from the GitHub REST API into a medallion DuckDB warehouse,
model with dbt, orchestrate with Airflow + Cosmos, visualize with Evidence.dev — all local, mostly
without Docker.

## Toolchain & commands

Python is managed with **uv**, pinned to **3.12** (dbt/Airflow don't yet support the system's
3.14). The commands below are exactly what CI runs (`.github/workflows/ci.yml`); they no-op until
the package is scaffolded, then activate automatically.

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

- **Ingestion is an Anti-Corruption Layer.** `github_ingest/` validates external GitHub JSON into
  typed Pydantic models *at the boundary* so upstream schema drift fails fast instead of
  corrupting silver. `BaseExtractor(ABC)` + per-entity subclasses; all DuckDB-specific SQL is
  isolated in `warehouse.py` (swap-friendly). The token is read from env via `SecretStr` and
  never logged or committed.

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
  above. **`dependabot.yml`** opens weekly grouped PRs for github-actions and pip; they go through
  the same gate.
- **No release/deploy pipeline** — this is a data project, not a published artifact.
- Phase 8 will add a `dbt` CI job running `dbt build` against a committed fixture DuckDB so CI
  needs no GitHub API access.
