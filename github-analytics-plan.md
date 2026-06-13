# GitHub Ecosystem Analytics — Project Plan & Design

> A portfolio data-engineering project built around one thesis — **stars measure hype, not
> health** — that ranks competing OSS data tools by dependency-risk signals. Ingest GitHub activity
> from the REST API into a medallion-architecture warehouse (DuckDB), transform with dbt Core,
> orchestrate with Airflow, and visualize with Evidence.dev — all running locally, mostly without
> Docker.

**Status:** phase 2 complete — ingestion package built and tested (54 tests), CI live.
**Target Python:** 3.12 (via `uv`; system is 3.14 which dbt/Airflow don't support yet).

---

## 1. The narrative

> *"Star counts measure hype, not health. Ingest a curated cohort of competing open-source
> data-tooling repos from the GitHub API, model their activity into a tested, documented,
> dimensional warehouse, and rank them by* dependency-risk *signals — bus factor, responsiveness,
> liveness, momentum — surfacing where a project's health disagrees with its popularity, refreshed
> daily so trend metrics accumulate real history."*

The thesis gives the project a point of view (a health *leaderboard*, not a descriptive dashboard),
and the build tells three engineering stories at once: **EL** (custom Python ingestion), **T** (dbt
medallion modeling), and **orchestration** (Airflow/Cosmos), topped with a **BI** layer (Evidence) —
the full stack.

**Scope boundary (own it, don't hide it):** the GitHub API exposes the maintainer/contributor side,
so the project measures project **health and resilience**, not adoption or market share. "Safe to
depend on" is answerable from this data; "winning the market" is not, and the project never claims
it — keeping the claim and the data exactly matched.

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
- **Repos analyzed:** `common/ingest/repos.json` is the ingestion source of truth for which repos
  to ingest (e.g. duckdb/duckdb, dbt-labs/dbt-core, pola-rs/polars). `dbt/seeds/repos.csv` is a
  planned phase-3 dbt seed for modeling lookups — it is not read by the ingestion package.
- **Config-as-data:** all tunable config lives in `common/*.json`; logic lives in classes; secrets
  (`github_token`) come from `ENV`/`.env` only and never appear in `common/`. Pydantic validates
  every JSON file on load.
- **Viz:** Evidence.dev (chosen over Superset — Superset is painful without Docker; Evidence
  reads DuckDB/parquet natively and deploys as a linkable static site).
- **Cosmos execution mode:** **local** with `ExecutionConfig.dbt_executable_path` pointing at a
  dedicated dbt venv (per Astronomer docs — simpler than `virtualenv` mode, avoids the
  Airflow↔dbt dependency conflict via two separate venvs).
- **Run modes:** `--mode daily` (default) uses conditional HTTP requests (ETag/Last-Modified) and a
  `since` watermark derived from MAX(`since_field`) seen per entity — avoiding redundant fetches.
  `--mode history` ignores stored state on read (full backfill) but stays append-only. Both are
  safe to re-run.
- **Structured JSON logging:** a zero-dependency `_JsonFormatter` in `__main__.py` emits one JSON
  object per log line with `timestamp`, `level`, `logger`, `message`, and `extra=` context keys
  (`load_id`, `repo`, `entity`, `mode`, `endpoint`). Tokens and payloads are never logged. Level
  is configured via `settings.json` `log_level` and overridden by `--log-level`.

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
├── common/                            # tunable config as JSON (not secrets)
│   ├── settings.json                  # non-secret scalars: github_base_url, page_size, request_timeout, max_retries, duckdb_path, log_level
│   └── ingest/
│       ├── repos.json                 # ingestion source of truth: which repos to ingest
│       └── entities.json             # declarative EntitySpec records for all four entities
│
├── github_ingest/                     # OOP ingestion package
│   ├── __init__.py
│   ├── common.py                      # EntitySpec (Pydantic) + load_entities / load_repos
│   ├── config.py                      # Settings (pydantic-settings): constructor > env/.env > common/settings.json > defaults
│   ├── models.py                      # Pydantic models = silver staging contract + drift detector
│   ├── client.py                      # GitHubClient (auth, pagination, rate-limit, retry)
│   ├── extractors.py                  # Extractor(spec, client) — single config-driven class
│   ├── warehouse.py                   # DuckDBConnector (DDL + idempotent writes)
│   ├── pipeline.py                    # IngestionPipeline (orchestrates extractors → warehouse)
│   └── __main__.py                    # CLI entrypoint
│
├── tests/
│   ├── conftest.py                    # fixtures: in-memory DuckDB, recorded API JSON
│   ├── fixtures/*.json                # captured sample API responses
│   ├── test_common.py                 # EntitySpec loader/validation + no-secret-in-JSON guardrail
│   ├── test_models.py
│   ├── test_client.py
│   ├── test_extractors.py
│   ├── test_warehouse.py
│   └── test_pipeline.py
│
├── dbt/                               # planned (phase 3–5)
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
├── airflow/                           # planned (phase 6)
│   ├── dags/github_analytics_dag.py   # Cosmos DbtTaskGroup + EL + export tasks
│   └── requirements-airflow.txt
│
├── reports/                           # planned (phase 7) — Evidence.dev project
│   └── pages/*.md
│
└── .github/workflows/ci.yml           # uv + dbt build on PR
```

---

## 6. Ingestion package — OOP design (`github_ingest/`)

Implements the **Anti-Corruption Layer** pattern: external JSON is always persisted as raw
bronze; Pydantic validates non-blocking to catch drift before it can corrupt silver transforms.

### Bronze DDL — fixed generic schema

All bronze tables share the same six-column layout (DDL is hardcoded in `warehouse.py`, not
derived from Pydantic models):

| Column | Type | Purpose |
|---|---|---|
| `load_id` | VARCHAR | UUID per pipeline run |
| `record_id` | VARCHAR | Natural key or `nokey:<hash>` fallback |
| `entity` | VARCHAR | Entity type / discriminator (e.g. `"issue"`, `"pull_request"`) |
| `source_repo` | VARCHAR | `owner/repo` |
| `ingested_at` | TIMESTAMPTZ | Wall-clock time of ingest |
| `payload` | JSON | Raw API response verbatim |

A separate `bronze.ingestion_state` table (primary key: `endpoint + params_hash`) stores ETag,
Last-Modified, and `since` values to drive conditional HTTP requests and avoid redundant fetches.

### Entities implemented

`Repo`, `Issue` (+ PR discriminator), `Commit`, `Contributor` — declared in `common/ingest/entities.json`
as `EntitySpec` records. The `issue_or_pr` discriminator strategy returns `"pull_request"` when
`pull_request` key is present in the raw payload, so both types share `bronze.issues` with the
`entity` column distinguishing them. Stargazer and a dedicated PR-detail extractor are deferred.

### `common.py` — `EntitySpec` + loaders

`EntitySpec` is a Pydantic model that mirrors one entry in `common/ingest/entities.json`:

| Field | Purpose |
|---|---|
| `name` | Entity name and default discriminator value |
| `table_name` | Target bronze table |
| `endpoint` | URL template (`/repos/{owner}/{name}/…`) |
| `key_field` | JSON field used as `record_id` |
| `single_object` | Fetch a single object instead of paginating |
| `supports_since` | Whether to send the `since` param on incremental runs |
| `since_field` | Dotted path to the timestamp field used as the watermark (e.g. `"updated_at"`, `"commit.committer.date"`); `null` for entities with no time-based incremental key |
| `paginate` | Whether to follow `Link: rel="next"` |
| `discriminator` | `"default"` or `"issue_or_pr"` |

`load_entities(path)` and `load_repos(path)` parse and validate the corresponding JSON files.

### `config.py` — `Settings(BaseSettings)`

Source priority: constructor args > env > `.env` > `common/settings.json` > field defaults.
`github_token: SecretStr | None` is sourced from env/`.env` only — it has no field in
`settings.json` and never appears in `common/`. A guardrail test asserts this invariant.
`SecretStr` keeps the token out of logs and reprs. `log_level` (default `"INFO"`) is read from
`common/settings.json` and overridden at runtime by `--log-level`.

### `client.py` — `GitHubClient`
Owns a `requests.Session` with the auth header.
- `get(path, params, etag, last_modified)` — single call; caller inspects the 304 status to skip.
- `paginate(path, params)` — generator following the `Link: rel="next"` header.
- Rate-limit aware: reads `X-RateLimit-Remaining`/`Reset`, sleeps when exhausted; retries 5xx +
  secondary-limit responses with exponential backoff (`tenacity`).

### `extractors.py` — `Extractor(spec, client)`

A single config-driven class replaces the former `BaseExtractor(ABC)` + per-entity subclasses:

```python
class Extractor:
    def __init__(self, spec: EntitySpec, client: GitHubClient) -> None: ...
    def endpoint(self, repo: str) -> str: ...                # resolves spec.endpoint template
    def record_id(self, raw: dict) -> str | None: ...        # reads spec.key_field
    def discriminator(self, raw: dict) -> str: ...           # "issue_or_pr" or default
    def validate(self, raw: dict) -> BaseModel | None: ...
    def paginate_raw(self, repo: str, *, etag, last_modified, params) -> PagedResult: ...
```

Two genuine logic branches remain in code: single-object fetch when `spec.single_object` is true,
and the `issue_or_pr` discriminator. The `since` param is only sent when `spec.supports_since` is
true (repos and contributors correctly omit it). For paginated entities, `paginate_raw` threads
conditional headers and `since` through the paged path; a `PagedResult.was_304` signals a 304 so
the pipeline skips the write entirely.

### `warehouse.py` — `DuckDBConnector`
Context manager around the DuckDB connection. `ensure_schema()` creates the `bronze` schema and
`ingestion_state` table. `ensure_bronze_table(table_name)` applies the fixed DDL.
`append(table_name, rows)` converts row dicts → PyArrow table → bulk `INSERT`. `get_state` /
`upsert_state` manage the conditional-request cache. All DuckDB-specific SQL is isolated here.

### `pipeline.py` — `IngestionPipeline`
`run(repos, full_refresh)` loops repos × extractors. For each extractor it reads state, issues a
conditional GET (ETag / Last-Modified / `since`), builds bronze rows with natural-key or
`nokey:<hash>` `record_id`, calls `validate()` non-blocking, appends to the bronze table, then
upserts state. The stored `since` watermark is the MAX value of `spec.since_field` seen across all
records in that run — not wall-clock time. Returns a `RunSummary` (rows by table, validation
failures, 304 skips). The DAG/CLI calls only this class.

### Testing strategy (real DB, no mock theater)
- **`test_common`** — `EntitySpec` loader/validation, settings-layering precedence, no-secret-in-JSON guardrail.
- **`test_models`** — validation, missing/bad fields, discriminator logic.
- **`test_client`** — pagination + rate-limit sleep path + 304 handling; HTTP mocked at the
  boundary (`responses` library).
- **`test_extractors`** — recorded JSON fixtures → correct `record_id` and `entity` values.
- **`test_warehouse`** — against a real in-memory DuckDB (`:memory:`): DDL, append, state
  upsert/read, `nokey` fallback. No DB mocking.
- **`test_pipeline`** — mocked client + in-memory DuckDB; assert row counts, 304 skips, and
  validation-failure counting.
- **`test_logging`** — verifies the JSON formatter output shape and that extra context keys land
  in the emitted record.
- 54 tests total; `ruff` + `mypy` in CI.

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

### Questions answered (the thesis, made concrete)

The headline payoff is a **health leaderboard that disagrees with the star ranking** — the
"stars lie, bus factor doesn't" finding:

- Which OSS data tool is **safest to depend on** — highest bus factor, not most stars?
- Where do **popularity and health diverge** (high stars, thin maintainer base — or the reverse)?
- Which project **responds to issues and merges PRs fastest**?
- Is a project's contributor base **growing or coasting** (e.g. DuckDB vs Polars momentum)?
- Who are the **cross-project contributors** linking the ecosystem (the optional influence-map
  second page)?

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
(pinned, checksum-verified `actionlint`) and `python` (uv + `ruff` + `mypy` + `pytest`; all
currently running and green against the ingestion package).

**Planned (phase 8):** add a `dbt` job that installs the dbt venv and runs `dbt build` against a
small committed fixture DuckDB / seeds, so CI needs no GitHub API access. Add a `docker` job that
builds the image and runs a smoke test, so "works in a container" is verified continuously rather
than at release time.

---

## 12. Build phases

1. ~~**Scaffold & env**~~ ✓ — uv + Python 3.12, `.gitignore`, `.env.example`, CI live.
2. ~~**Ingestion**~~ ✓ — `github_ingest/` package + 54 tests; config-as-data architecture (`common/settings.json` + `common/ingest/{repos,entities}.json` + Pydantic loaders + single `Extractor` class); bronze loads from the GitHub API; incremental via conditional requests + max-seen watermark; structured JSON logging.
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
