# oss_pulse

[![CI](https://github.com/alexeyserdtse/oss_pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/alexeyserdtse/oss_pulse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](github-analytics-plan.md)
![Status: in development](https://img.shields.io/badge/status-in%20development-orange.svg)

**Star counts measure hype, not health.** A project with 70k stars can be one burned-out maintainer
away from abandonment, while a quieter one is a safe bet for years. oss_pulse ranks competing
open-source data tools by the signals that actually predict whether a project is **safe to build
on** — bus factor, issue responsiveness, and contributor resilience — and shows where popularity and
health *disagree*.

The technical version: an end-to-end pipeline that ingests GitHub REST API activity into a medallion
[DuckDB](https://duckdb.org) warehouse, models it with [dbt](https://www.getdbt.com), orchestrates
with Airflow + [Cosmos](https://www.astronomer.io/cosmos/), and surfaces the rankings in
[Evidence.dev](https://evidence.dev) dashboards — all running locally, mostly without Docker.

> **Status:** ingestion complete. The `github_ingest/` package loads bronze from the GitHub REST
> API into DuckDB and has a 54-test suite. The dbt transform layer, Airflow orchestration, and
> Evidence dashboards are not yet built. See [`github-analytics-plan.md`](github-analytics-plan.md)
> for the full phased plan.

## What it demonstrates

- **Custom EL** *(built)* — a Python ingestion package (`github_ingest/`) with a config-as-data
  architecture: all tunable settings, repo lists, and entity specs live in `common/settings.json`
  and `common/ingest/{repos,entities}.json` (Pydantic-validated on load); a single `Extractor`
  class drives all four entities from those specs. Raw GitHub API responses land in a fixed-schema
  bronze layer in DuckDB. Pydantic validates non-blocking as a drift detector and silver staging
  contract. Ingestion is incremental by default (`--mode daily`): ETag/Last-Modified conditional
  requests and a `since` watermark derived from the MAX timestamp seen per entity avoid redundant
  fetches; `--mode history` runs a full backfill. Structured JSON logging is emitted to stderr with
  configurable level (`settings.json` `log_level` / `--log-level`); tokens and payloads are never
  logged. 54 tests (real in-memory DuckDB, HTTP mocked at the boundary).
- **Dimensional modeling** *(planned)* — a dbt medallion (bronze → silver → gold) with snapshots
  for slowly-changing history.
- **Orchestration** *(planned)* — a daily Airflow DAG running dbt through Cosmos, each model a
  native task.
- **BI** *(planned)* — Evidence.dev dashboards reading gold parquet, deployable as a static site.

## Architecture

```
                 ┌─────────────────── Airflow DAG (daily, sequential) ───────────────────┐
GitHub REST API  │  extract_github → dbt_snapshot → dbt_build (run+test) → export_parquet │
   (PAT, free)   │     │                                                       │          │
                 └─────┼───────────────────────────────────────────────────────┼─────────┘
                       ▼                                                         ▼
                  DuckDB file ── medallion: bronze ▸ silver ▸ gold ──▶ gold/*.parquet ──▶ Evidence.dev
```

Sequential by design: DuckDB is single-writer, so the DAG runs tasks in series and the final step
exports gold marts to parquet — Evidence reads the parquet, never the live database, sidestepping
lock contention.

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Env / Python | **uv** (Python 3.12) | reproducible, lockfile-pinned venvs |
| Ingestion (EL) | **Python + requests + Pydantic v2** | schema-validated extraction at the API boundary |
| Warehouse | **DuckDB** | fast columnar analytics in a single file |
| Transform | **dbt-core + dbt-duckdb** | tested, documented medallion models |
| Orchestration | **Airflow + Cosmos** | daily DAG; dbt models render as native Airflow tasks |
| Visualization | **Evidence.dev** | SQL + markdown dashboards, deployed as a static site |
| CI | **GitHub Actions** | lint, type-check, and test on every PR |

## The thesis: stars lie, bus factor doesn't

Most teams pick a dependency by popularity — stars, mindshare, "everyone uses it." But popularity
doesn't tell you whether the project will still be maintained, or how fast a critical bug gets
triaged. oss_pulse replaces the gut call with measured **dependency-risk** signals across a curated
cohort of competing tools (currently [duckdb](https://github.com/duckdb/duckdb),
[polars](https://github.com/pola-rs/polars), and [dbt-core](https://github.com/dbt-labs/dbt-core) —
add or remove repos by editing one JSON file).

The payoff is a **health leaderboard** that often disagrees with the star ranking. The gold marts
answer:

- **Resilience** — what's the *bus factor*? If the top 3 contributors walked away, does the project
  survive?
- **Responsiveness** — how fast does a project triage issues and merge PRs? (median time-to-first-
  response, median merge time)
- **Liveness** — is it still actively maintained? (commit cadence, days since last commit, % issues
  left open)
- **Momentum** — is the contributor base growing or coasting? (new contributors and commit trends
  over time)

**Scope, honestly stated:** the GitHub API exposes the *maintainer/contributor* side, so this
measures project **health and resilience** — not adoption or market share. "Safe to depend on" is
answerable from this data; "winning the market" is not, and the project doesn't claim it.

## CI/CD

CI runs on every pull request and push to `master` ([`ci.yml`](.github/workflows/ci.yml)):

- **`lint-workflows`** — lints the workflows with a checksum-pinned `actionlint`.
- **`python`** — sets up uv, then runs `ruff`, `mypy`, and `pytest` against the ingestion package.

[Dependabot](.github/dependabot.yml) opens weekly grouped PRs for GitHub Actions and pip updates,
which pass through the same gate. `master` is protected: force-push and deletion are blocked, and
merges require both checks green via a pull request. There is no release/deploy pipeline — this is
a data project, not a published artifact.

## Documentation

- [`github-analytics-plan.md`](github-analytics-plan.md) — full design: stack decisions, repo
  layout, ingestion and dbt structure, gold marts, and build phases.
- [`CLAUDE.md`](CLAUDE.md) — operational guidance for working in this repository.

## License

[MIT](LICENSE).
