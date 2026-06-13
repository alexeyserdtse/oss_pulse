# GitHub Ecosystem Analytics — Project Plan & Design

> A portfolio data-engineering project: ingest GitHub activity from the REST API into a
> medallion-architecture warehouse (DuckDB), transform with dbt Core, orchestrate with
> Airflow, and visualize with Evidence.dev — all running locally, mostly without Docker.

**Status:** planning complete, not yet scaffolded.
**Target Python:** 3.12 (via `uv`; system is 3.14 which dbt/Airflow don't support yet).

---

## 1. The narrative

> *"Ingest a curated set of open-source data-tooling repos from the GitHub API, model their
> activity into a tested, documented, dimensional warehouse, and surface contributor & repo
> health metrics — refreshed daily so trend/growth metrics accumulate real history."*

This tells three stories at once: **EL** (custom Python ingestion), **T** (dbt medallion modeling),
and **orchestration** (Airflow/Cosmos), topped with a **BI** layer (Evidence) — the full stack.

---

## 2. Stack & key decisions

| Layer | Tool | Docker? | Notes |
|---|---|---|---|
| Env / Python mgmt | **uv** | No | manages Python 3.12 + venvs locally |
| Ingestion (EL) | **Python + requests + Pydantic v2** | No | OOP package, schema-validated |
| Warehouse | **DuckDB** (single file) | No | columnar, single-writer |
| Transform | **dbt-core + dbt-duckdb** | No | medallion via folders + schemas |
| Orchestration | **Airflow (pip `standalone`) + Cosmos** | No | local execution mode |
| Visualization | **Evidence.dev** (npm) | No | reads gold parquet; deploys as static site |
| CI | **GitHub Actions** | No | `dbt build` on PR against a fixture |
| Quality | **pytest, pytest-cov, ruff, mypy** | No | portfolio-grade signal |

### Decisions locked
- **Data source:** GitHub REST API. Free. Unauth = 60 req/hr; with a free Personal Access
  Token (no scopes needed for public data) = 5,000 req/hr. Token read from env, never committed.
- **Repos analyzed:** curated `seeds/repos.csv` of ~8–12 OSS data-tooling repos
  (e.g. duckdb/duckdb, dbt-labs/dbt-core, apache/airflow, pola-rs/polars, tobymao/sqlglot,
  apache/superset, evidence-dev/evidence, …). Editable.
- **Viz:** Evidence.dev (chosen over Superset — Superset is painful without Docker; Evidence
  reads DuckDB/parquet natively and deploys as a linkable static site).
- **Cosmos execution mode:** **local** with `ExecutionConfig.dbt_executable_path` pointing at a
  dedicated dbt venv (per Astronomer docs — simpler than `virtualenv` mode, avoids the
  Airflow↔dbt dependency conflict via two separate venvs).

### Decisions still open
1. **Bronze load strategy** — recommendation: **append-only with `ingested_at`** (preserves
   bronze fidelity/auditability; dbt snapshot handles SCD; silver dedupes to latest-per-key).
   Alternative: replace-each-run (bounded size, also defensible).
2. **HTTP client** — recommendation: **`requests` (sync)** for simplicity. `httpx` (async) only
   if ingestion of many repos becomes slow.
3. **GitHub token** — do you have a PAT? Pipeline will run token-less at 60 req/hr as a fallback.

---

## 3. Architecture

```
                 ┌──────────────────── Airflow DAG (daily, sequential) ────────────────────┐
GitHub REST API  │  extract_github → dbt_snapshot → dbt_build (run+test) → export_parquet   │
   (PAT, free)   │     │                                                        │           │
                 └─────┼────────────────────────────────────────────────────────┼──────────┘
                       ▼                                                          ▼
                  DuckDB file  ── medallion: bronze ▸ silver ▸ gold ──▶  gold/*.parquet ──▶ Evidence.dev
```

**Why sequential + parquet export:** DuckDB is single-writer (exclusive file lock). The DAG runs
tasks in series so there's never a concurrent writer, and the final step exports gold marts to
parquet so Evidence reads parquet — never the live `.duckdb` file — sidestepping all lock contention.

---

## 4. Medallion ↔ dbt mapping

| Medallion | Contents | dbt mechanism | DuckDB schema |
|---|---|---|---|
| **Bronze** | Raw API payloads, exactly as received + `ingested_at`, `source_repo` | Python EL writes; dbt reads via `sources` | `bronze` |
| **Silver** | Cleaned/typed (`stg_`) + joined/derived (`int_`) | staging + intermediate models | `silver` |
| **Gold** | Dimensions, facts, business marts | `dim_` / `fct_` / `mart_` models | `gold` |
| **History** | SCD2 on stars/forks | dbt **snapshot** (`snap_repo_stars`) | `snapshots` |

Schemas pinned in `dbt_project.yml`:
```yaml
models:
  github_analytics:
    silver:
      +schema: silver
    gold:
      +schema: gold
```

**Principle (from research):** three layers is the right number. Anything between silver and gold
goes in dbt **intermediate** models — do not invent a fourth layer. Keep dbt's `stg_/int_/fct_/dim_`
naming inside the medallion folders; don't fight the convention.

---

## 5. Repo layout

```
oss_pulse/
├── pyproject.toml / uv.lock           # uv-managed
├── .python-version                    # 3.12
├── README.md                          # architecture, lineage, screenshots, run guide
├── .env.example                       # GITHUB_TOKEN=...
├── .gitignore                         # *.duckdb, .env, target/, .venv*, node_modules, dbt_packages/
│
├── github_ingest/                     # OOP ingestion package
│   ├── __init__.py
│   ├── config.py                      # Settings (pydantic-settings)
│   ├── models.py                      # Pydantic schemas = bronze contract
│   ├── client.py                      # GitHubClient (auth, pagination, rate-limit, retry)
│   ├── extractors.py                  # BaseExtractor(ABC) + per-entity subclasses
│   ├── warehouse.py                   # DuckDBConnector (DDL + idempotent writes)
│   ├── pipeline.py                    # IngestionPipeline (orchestrates extractors → warehouse)
│   └── __main__.py                    # CLI entrypoint
│
├── tests/
│   ├── conftest.py                    # fixtures: in-memory DuckDB, recorded API JSON
│   ├── fixtures/*.json                # captured sample API responses
│   ├── test_models.py
│   ├── test_client.py
│   ├── test_extractors.py
│   ├── test_warehouse.py
│   └── test_pipeline.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                   # duckdb adapter, file-based
│   ├── packages.yml                   # dbt_utils, dbt_expectations
│   ├── models/
│   │   ├── bronze/_bronze__sources.yml        # sources only (raw loaded by Python)
│   │   ├── silver/
│   │   │   ├── staging/   stg_github__*.sql + _github__models.yml
│   │   │   └── intermediate/ int_*.sql + _int__models.yml
│   │   └── gold/
│   │       ├── dim_repo.sql, dim_contributor.sql
│   │       ├── fct_commits.sql, fct_issues.sql
│   │       ├── mart_repo_health.sql, mart_contributor_activity.sql, mart_repo_trends.sql
│   │       └── _gold__models.yml + _gold__docs.md
│   ├── snapshots/snap_repo_stars.sql
│   ├── macros/                        # 1 reusable macro + 1 custom generic test
│   ├── seeds/repos.csv                # curated repo list (+ language→category lookup)
│   └── tests/                         # singular tests
│
├── airflow/
│   ├── dags/github_analytics_dag.py   # Cosmos DbtTaskGroup + EL + export tasks
│   └── requirements-airflow.txt
│
├── reports/                           # Evidence.dev project (reads gold parquet)
│   └── pages/*.md
│
└── .github/workflows/ci.yml           # uv + dbt build on PR
```

---

## 6. Ingestion package — OOP design (`github_ingest/`)

Implements the **Anti-Corruption Layer** pattern: external JSON is validated into typed Pydantic
models at the boundary, so upstream schema drift fails fast instead of corrupting silver.

### `config.py` — `Settings(BaseSettings)`
Env-driven single source of truth (`.env`): `github_token: SecretStr`, `github_base_url`,
`page_size=100`, `request_timeout`, `max_retries`, `duckdb_path`, repo list (from `repos.csv`).
`SecretStr` keeps the token out of logs and reprs.

### `models.py` — the bronze schema (Pydantic v2)
One model per entity: `Repo`, `Commit`, `Issue`, `PullRequest`, `Stargazer`, `Contributor`.
A `BronzeRecord` mixin adds `ingested_at: datetime` and `source_repo: str`. Field aliases map
GitHub JSON; each model declares `__tablename__` + a column spec so the warehouse derives stable
DuckDB DDL (explicit typing, not inference).

### `client.py` — `GitHubClient`
Owns a `requests.Session` with the auth header.
- `get(path, params)` — single call, raises on HTTP error.
- `paginate(path, params)` — generator following the `Link: rel="next"` header.
- Rate-limit aware: reads `X-RateLimit-Remaining`/`Reset`, sleeps when exhausted; retries 5xx +
  secondary-limit responses with exponential backoff (`tenacity`).

### `extractors.py` — `BaseExtractor(ABC)`
Six near-identical extractors justify the abstraction:
```python
class BaseExtractor(ABC):
    model: type[BronzeRecord]
    table_name: str
    @abstractmethod
    def endpoint(self, repo: str) -> str: ...
    def parse(self, raw: dict, repo: str) -> BronzeRecord:   # default: model.model_validate
    def extract(self, repo: str) -> Iterator[BronzeRecord]:  # paginate → parse → yield
```
Subclasses declare `model`, `table_name`, `endpoint()`. `RepoExtractor` overrides `extract`
(single object, not a list).

### `warehouse.py` — `DuckDBConnector`
Context manager around the connection. `ensure_schema("bronze")`, `create_table(model)` from the
column spec, `write(table, records, mode)` converting validated records → PyArrow table →
`INSERT` / `CREATE OR REPLACE`. All DuckDB-specific SQL is isolated here (swap-friendly).

### `pipeline.py` — `IngestionPipeline`
`run()` loops repos × extractors, stamps `ingested_at`, logs row counts. The only class the DAG/CLI
calls.

### Testing strategy (real DB, no mock theater)
- **`test_models`** — validation, aliases, missing/bad fields raise.
- **`test_client`** — pagination + rate-limit sleep path via `responses`/`requests-mock`
  (HTTP is the boundary — mock *there*).
- **`test_extractors`** — recorded JSON fixtures → correct model instances.
- **`test_warehouse`** — against a **real in-memory DuckDB** (`:memory:`): DDL, writes,
  `ingested_at`, idempotent re-run. No DB mocking.
- **`test_pipeline`** — mocked client + in-memory DuckDB; assert row counts land in bronze.
- Plus `ruff` + `mypy` in CI.

---

## 7. dbt layer

- **Sources** (`_bronze__sources.yml`): declare bronze tables with freshness + descriptions.
- **Staging** (`stg_github__*`): 1:1 with sources, light transforms only (rename, cast, clean).
  **No joins** — those go in intermediate.
- **Intermediate** (`int_*`): the heavy lifting — `int_issue_response_times`, `int_pr_lifecycle`,
  `int_commit_enriched`. Each derivation computed in exactly one place (DRY).
- **Marts/gold** (`dim_`, `fct_`, `mart_`): organized by domain.
- **Tests:** generic (`unique`, `not_null`, `relationships`, `accepted_values`) on keys + critical
  columns; `dbt_utils` / `dbt_expectations`; 1 custom **generic** test; 1–2 **singular** tests.
- **Snapshot:** `snap_repo_stars` (SCD2 on stars/forks) so growth/trend metrics gain history.
- **Macro:** 1 reusable macro (e.g. a date spine or a duration-in-hours helper).
- **Seeds:** `repos.csv` + a language→category lookup.
- **Docs:** descriptions in YAML; `dbt docs generate` → capture lineage graph for README.

---

## 8. Analytics — gold marts

### `mart_repo_health` (one row per repo)
- **Popularity:** stars, forks, watchers, star-growth rate (needs snapshot).
- **Activity:** commits last 30/90 days, days since last commit.
- **Responsiveness:** median time-to-first-response on issues, median close time, % still open.
- **PR throughput:** median merge time, open vs merged ratio.
- **Community:** contributor count, **bus factor** (% of commits from top 3 contributors).

### `mart_contributor_activity` (one row per contributor × repo)
- Commits / PRs / issues; first-seen, last-seen, active span.
- Core vs drive-by (commit-count threshold); cross-repo contributors.

### `mart_repo_trends` (repo × month)
- Commits/month, new contributors/month, issues opened vs closed/month.
- The charting mart; pays off the snapshot.

### Questions answered
- Which OSS data tool has the healthiest contributor base (lowest bus factor)?
- Which project responds to issues fastest?
- Is DuckDB's contributor growth accelerating vs Polars?
- Who are the cross-project contributors connecting the ecosystem?

**Caveat:** growth/trend metrics need accumulated history the API can't give retroactively —
they fill in once the daily snapshot has run for a while. Everything else (response times, bus
factor, throughput) derives from per-event timestamps the API gives immediately. Optionally seed a
little synthetic history so charts aren't empty on day one.

---

## 9. Orchestration — Airflow + Cosmos

Single daily DAG, **sequential** (DuckDB single-writer):
```
extract_github → dbt_snapshot → dbt_build (run + test) → export_parquet
```
- Airflow installed via pip, run with `airflow standalone` (no Docker).
- **Two venvs** (`.venv-airflow`, `.venv-dbt`) to avoid the dependency conflict.
- Cosmos in **local execution mode**, `ExecutionConfig(dbt_executable_path=<dbt venv>/bin/dbt)`,
  wired with `DbtTaskGroup` + `ProjectConfig` + `ProfileConfig` + `ExecutionConfig` so each dbt
  model renders as its own Airflow task (lineage visible inside the Airflow graph).
- Each run: pull fresh API data → snapshot point-in-time stars/forks → rebuild silver/gold + test →
  export gold to parquet for Evidence.

---

## 10. Visualization — Evidence.dev

- npm project under `reports/`; reads gold **parquet** exports (avoids DuckDB lock contention).
- Dashboards as SQL + markdown (version-controlled).
- 2–3 pages: ecosystem overview, repo health scorecard, contributor deep-dive.
- Deploys as a **static site** — linkable from résumé/portfolio.
- (If Superset specifically wanted on résumé, that's the one place to allow Docker:
  `docker compose up`.)

---

## 11. CI — GitHub Actions

**Current (`ci.yml`):** two jobs run on every PR and push to `master` — `lint-workflows`
(pinned, checksum-verified `actionlint`) and `python` (uv + `ruff` + `mypy` + `pytest`).
The `python` job no-ops cleanly until the package is scaffolded.

**Planned (phase 8):** add a `dbt` job that installs the dbt venv and runs `dbt build` against a
small committed fixture DuckDB / seeds, so CI needs no GitHub API access. Add a `docker` job that
builds the image and runs a smoke test, so "works in a container" is verified continuously rather
than at release time.

---

## 12. Build phases

1. **Scaffold & env** — uv + Python 3.12, two venvs, dbt init, DuckDB profile, `.gitignore`,
   `.env.example`. Verify dbt + DuckDB connect.
2. **Ingestion** — build `github_ingest/` package + tests; run once to populate bronze.
3. **Bronze→Silver** — `sources.yml` + `stg_*` + `int_*`.
4. **Gold** — dims, facts, the three marts.
5. **Tests / seeds / macros / snapshot** — generic + singular tests, macro, custom generic test,
   `snap_repo_stars`.
6. **Orchestration** — Airflow standalone + Cosmos DAG; confirm full daily run end-to-end.
   Introduce **Docker here**: Airflow is the one component painful to run locally (pip
   `standalone`, two venvs to dodge the dbt conflict), and the official Airflow images/compose are
   well-trodden — containerizing at this phase removes the biggest local-setup headache.
7. **Visualization** — Evidence project reading gold parquet; 2–3 pages.
8. **Docs & CI & README** — lineage screenshot, GitHub Actions, polished README. Add a
   `Dockerfile` (+ compose) as the deliverable/demo layer so a reviewer can `docker compose up`,
   and a CI `docker` job that builds + smoke-tests the image.

### Containerization strategy

Develop locally with uv (fast iteration); **stay portable throughout** — env-driven config, no
hardcoded absolute paths, pinned Python — so containerizing is cheap, not a big-bang final step.
uv (reproducible venvs) + DuckDB (embedded single file) already cover most of Docker's isolation
benefit, so Docker's role here is the **demo/release artifact and the Airflow runtime**, not daily
dev. Bring it in at phase 6, verify it in CI at phase 8.

---

## 13. Best-practice findings (research, with sources)

**dbt structure (dbt Labs official):** source-conformed → business-conformed arc; three layers;
hard naming conventions (`stg_<source>__<entity>`, `int_<purpose>`, `fct_`/`dim_`,
`base_` for pre-clean); staging is 1:1 with sources with no joins; each transform in exactly one
place; YAML co-located and underscore-prefixed (`_<source>__sources.yml`, `_<source>__models.yml`);
marts organized by business domain.

**Medallion:** bronze's goal is *fidelity* — store exactly what the source sent + metadata to
replay/audit; enforce schema at the bronze write path or via dbt sources to fail fast on drift;
three layers is the right number — extra steps go in dbt intermediate, not a new layer.

**Pydantic / Anti-Corruption Layer:** validate external data into typed models at the ingestion
boundary ("schema-on-read at ingestion"); ad-hoc dicts in the extraction layer are technical debt.

**Cosmos/Airflow:** prefer **local execution mode** with `ExecutionConfig.dbt_executable_path`
over virtualenv mode — lets you manage the dbt env while keeping Airflow simpler; use a separate
venv for dbt to avoid dependency conflicts.

### Sources
- How we structure our dbt projects — https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview
- Intermediate models — https://docs.getdbt.com/best-practices/how-we-structure/3-intermediate
- Medallion locally with dbt + DuckDB — https://blog.dataengineerthings.org/how-to-build-a-medallion-architecture-locally-with-dbt-and-duckdb-e8e5e9270a72
- Medallion in practice (Bronze/Silver/Gold) — https://www.ryankirsch.dev/blog/medallion-architecture-data-engineering
- Cosmos execution modes — https://astronomer.github.io/astronomer-cosmos/getting_started/execution-modes.html
- Orchestrate dbt Core with Airflow + Cosmos — https://www.astronomer.io/docs/learn/airflow-dbt
- Pydantic for data projects (ACL pattern) — https://data-ai.theodo.com/en/technical-blog/boost-your-data-projects-with-pydantic-validation-and-efficiency
- DuckDB & Python end-to-end project — https://motherduck.com/blog/duckdb-python-e2e-data-engineering-project-part-1/
