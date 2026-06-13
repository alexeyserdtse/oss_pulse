# oss_pulse

GitHub ecosystem analytics pipeline — ingest GitHub activity into a medallion-architecture
DuckDB warehouse, transform with dbt, orchestrate with Airflow, visualize with Evidence.dev.
See [`github-analytics-plan.md`](github-analytics-plan.md) for the full design.

## CI/CD

Continuous integration runs on GitHub Actions; there is no deployment/release pipeline (this is
a data project, not a published artifact).

### Workflows

**`.github/workflows/ci.yml`** runs on every pull request and on pushes to `master`, with two jobs:

| Job | What it does |
|---|---|
| `lint-workflows` | Lints the workflow YAML itself with a pinned `actionlint` (checksum-verified download). |
| `python` | Detects whether Python code exists; if so, sets up [uv](https://docs.astral.sh/uv/) (Python from `.python-version`), runs `uv sync --all-extras`, then `ruff check` + `ruff format --check`, `mypy`, and `pytest`. No-ops cleanly until the package lands. |

Concurrency is capped per ref (`cancel-in-progress`) and the default token is read-only.

### Dependency updates

**`.github/dependabot.yml`** opens grouped PRs weekly for `github-actions` and `pip` updates.
These pass through the same CI and branch protection — merge them as they arrive.

### Branch protection (`master`)

- Force-push and deletion are blocked.
- Merges require the `lint-workflows` and `python` checks to pass, with the branch up to date.
- A pull request is required before merging (0 approvals — solo-friendly).
- Admins are **not** enforced, leaving the owner an emergency direct-push override
  (GitHub logs such a push as a bypass rather than rejecting it).

### Local guard

A global Claude Code `PreToolUse/Bash` hook blocks force-pushes whose target ref is `main`/`master`
(plus `rm -rf /`, `DROP DATABASE/TABLE`, `mkfs.`, `dd of=/dev/…`) before they run.

> Planned (phase 8): a `dbt` job running `dbt build` against a committed fixture DuckDB so CI
> needs no GitHub API access.
