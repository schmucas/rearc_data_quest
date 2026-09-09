# rearc_data_quest

A Databricks Declarative Automation Bundle (DAB) project.

Unity Catalog layout: `rearc_dev`, `rearc_stage`, `rearc_prod`, each with
`bronze` / `silver` / `gold` schemas. Raw source data lands in `rearc_ingest`.

## Quickstart

```bash
cp .envrc.example .envrc      # fill in host + PAT, then: direnv allow
uv sync --group dev

databricks bundle validate --target dev
databricks bundle deploy --target dev

databricks bundle run setup_job --target dev              # once, destructive
databricks bundle run bronze_ingestion_job --target dev
databricks bundle run declarative_pipeline_silver_gold --target dev

uv sync --group sourcing
BLS_CONTACT_EMAIL=you@example.com uv run python -m sourcing --env dev
```

## Layout

```
databricks.yml                 bundle definition: env + catalog_prefix, 3 targets
resources/                     one file per job or pipeline
sourcing/                      GitHub Actions fetcher: BLS + DataUSA -> landing volume + manifest (not deployed by the bundle)
src/setup/                     environment creation and seed data
src/ingestion/                 bronze: Auto Loader and Delta CDF patterns
src/silver/                    declarative pipeline: typed, deduplicated, CDC applied
src/gold/                      declarative pipeline: dimensional model
src/maintenance/               OPTIMIZE / VACUUM
src/utils/                     importable helpers, unit tested
tests/                         bundle guardrails (pure Python) + utils unit tests
.github/workflows/             pr, deploy-dev, deploy-stage, deploy-prod
```

## CI/CD

Trunk based: a single `main` plus short lived feature branches. The three deploy
targets are bundle targets, not git branches.

| Trigger | Workflow | Effect |
|---|---|---|
| PR to `main` | `pr.yml` | ruff, `bundle validate --target dev`, pytest, summary report |
| Merge to `main` | `deploy-dev.yml` | deploy to `dev` |
| Tag `v*-rc*` or manual dispatch | `deploy-stage.yml` | deploy to `stage` |
| Tag `v*` or manual dispatch | `deploy-prod.yml` | gated deploy to `prod` via the `production` GitHub Environment |
| Manual dispatch | `sourcing.yml` | fetch BLS + DataUSA into `landing/` and `source_manifest` (dev only, no schedule yet) |

Repository secrets required: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.
Repository variable required: `BLS_CONTACT_EMAIL` (Settings → Secrets and
variables → Actions → **Variables** tab, not Secrets).

See [CLAUDE.md](CLAUDE.md) for the conventions this project holds itself to, and
[docs/mcp-setup.md](docs/mcp-setup.md) for wiring the Databricks MCP server.
