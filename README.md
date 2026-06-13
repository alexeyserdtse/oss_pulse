# oss_pulse

[![CI](https://github.com/alexeyserdtse/oss_pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/alexeyserdtse/oss_pulse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](github-analytics-plan.md)
![Status: in development](https://img.shields.io/badge/status-in%20development-orange.svg)

End-to-end **GitHub ecosystem analytics** pipeline: ingest activity from the GitHub REST API into a
medallion [DuckDB](https://duckdb.org) warehouse, model it with [dbt](https://www.getdbt.com),
orchestrate with Airflow + [Cosmos](https://www.astronomer.io/cosmos/), and surface repo-health and
contributor metrics in [Evidence.dev](https://evidence.dev) dashboards — all running locally, mostly
without Docker.

> **Status:** design complete, implementation in progress. The CI/CD foundation is live; the
> pipeline is being built out per the phased plan in
> [`github-analytics-plan.md`](github-analytics-plan.md).

## What it demonstrates

- **Custom EL** — an OOP Python ingestion package (Pydantic-validated, rate-limit-aware) that
  treats the GitHub API as an anti-corruption boundary, so upstream schema drift fails fast.
- **Dimensional modeling** — a tested, documented dbt medallion (bronze → silver → gold) with
  snapshots that accumulate slowly-changing history.
- **Orchestration** — a daily Airflow DAG that runs dbt through Cosmos, each model a native task.
- **BI** — Evidence.dev dashboards reading gold parquet, deployable as a linkable static site.

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

## Analytics

The gold marts are designed to answer questions like:

- Which OSS data tool has the healthiest contributor base (lowest bus factor)?
- Which project responds to issues fastest?
- Is DuckDB's contributor growth accelerating versus Polars?
- Who are the cross-project contributors linking the ecosystem together?

## CI/CD

CI runs on every pull request and push to `master` ([`ci.yml`](.github/workflows/ci.yml)):

- **`lint-workflows`** — lints the workflows with a checksum-pinned `actionlint`.
- **`python`** — sets up uv, then runs `ruff`, `mypy`, and `pytest`. It no-ops cleanly until the
  package is scaffolded, then activates automatically.

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
