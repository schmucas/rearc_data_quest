# CLAUDE.md — Databricks project

## Project shape

- **Databricks:** Free Edition (the 2025 serverless + UC free tier, NOT legacy
  Community Edition). Serverless compute only. No clusters to configure.
- **Bundle:** Declarative Automation Bundles (DABs, formerly Databricks Asset
  Bundles). Three targets: `dev`, `stage`, `prod`.
- **No wheel.** There is no `artifacts` block, no `python_wheel_task`, and no
  `src/<pkg>/` installable package. Do not reintroduce any of these. Shareable,
  unit-testable logic goes in a separate utils wheel repo, or in `src/utils/`
  for helpers that only this project uses.
- **Pipeline runs off** the declarative `src/silver/*.py` and `src/gold/*.py`
  files via the glob include in `resources/declarative_pipeline.yml`.
- **Jobs are not pipeline-only.** A job may mix `notebook_task`, `pipeline_task`
  (refresh), `run_job_task`. Notebook tasks are expected and fine; the only
  banned task type is `python_wheel_task`.

## Hard conventions (do not violate, do not suggest violating)

- **Python pipelines only.** Pipeline logic is `pyspark.pipelines` (`@dp.table`,
  never the legacy `@dlt.table` spelling). No SQL pipelines, ever. This scopes
  the declarative pipeline; it does not forbid ordinary PySpark in job notebooks.
- **Transformations stay declarative.** Don't turn transformation files into
  importable, reconfigurable modules. They use `@dp.*` decorators and a global
  `spark`; they run inside the pipeline, they are not imported.
- **No hardcoded catalog names.** The catalog is derived at runtime from two
  values passed in by the bundle: `catalog_prefix` and `env`. Notebooks read them
  from `dbutils.notebook.entry_point.getCurrentBindings()`; pipeline source files
  read them with `spark.conf.get(...)`. Never write a literal `rearc_dev`.
- **No DBFS.** Unity Catalog only: catalog tables and
  `/Volumes/<catalog>/<schema>/<volume>/` paths. Never `/tmp`, `/dbfs/`,
  `/mnt/`, or `dbutils.fs.*` for storage.

## Layer conventions

- **bronze** — raw landing, append-only, `_ingested_at` / `_source_file` audit
  columns, `_rescued_data` from Auto Loader. No business logic.
- **silver** — typed, deduplicated, CDC-applied. Written by the pipeline; bare
  dataset names resolve here because the pipeline sets `schema: silver`.
- **gold** — dimensional model (facts, dims, aggregates). Fully qualified names,
  since only silver is bare.

## Testing

- `tests/test_bundle_config.py` is pure Python (no Spark, no Databricks Connect).
  It runs in seconds in CI and guards the conventions above. Extend it when a new
  convention is worth enforcing.
- `tests/conftest.py` provides a `spark` fixture via Databricks Connect. Anything
  using it consumes serverless DBUs on every PR run, so add such tests
  deliberately.
- Don't try to unit-test the declarative `@dp.*` files directly.

## CI/CD (delegated area — you may author here)

Trunk-based: a single `main` plus short-lived feature branches. Deploy targets
are bundle targets, not git branches.

- **PR** → ruff, `bundle validate --target dev`, pytest, summary report.
- **Merge to `main`** → auto-deploy to `dev`.
- **Tag `v*-rc*`** or manual dispatch → deploy to `stage`, run integration tests.
- **Tag `v*`** → gated deploy to `prod` via a GitHub Environment approval gate.
- **Free Edition note:** DABs + CI/CD work via PAT + serverless. OAuth M2M does
  not (no account console). Use PAT auth in Actions.
- **GitHub Free note:** Environment required reviewers only work in PUBLIC
  repositories. In a private repo the `production` environment exists and the
  deploy still runs, unattended, with no approval step.

## Security and privacy

- Flag security vulnerabilities you notice and point them out.
- Don't let PII, credentials, or other sensitive values land in this repo.
- Act as a placeholder for security scanning and watch for:
  - code and application vulnerabilities
  - secrets and credentials
  - open-source and dependency risk (SCA)
  - infrastructure and cloud misconfiguration (IaC)
