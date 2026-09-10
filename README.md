<div align="center">

# Rearc Data Quest: Databricks Edition

Sourcing two public datasets onto Databricks and modelling them through a
bronze, silver, and gold medallion pipeline, deployed as a Declarative
Automation Bundle (DAB) on Databricks Free Edition.

![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-2088FF?logo=githubactions&logoColor=white)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4)](https://delta.io)
[![Built with Claude Code](https://img.shields.io/badge/built%20with-claude%20code-CC785C?logo=claude&logoColor=white)](https://claude.com/claude-code)

</div>

## Built with Claude Code

This project is built in close collaboration with Claude Cowork and Claude
Code.

Claude Code sessions run on a set of custom plugins and slash commands,
published in a personal marketplace repo,
[schmucas/dotclaude](https://github.com/schmucas/dotclaude), alongside
Databricks' own officially maintained `databricks` plugin (DABs, Lakeflow
Jobs, Spark Declarative Pipelines, Unity Catalog, and more).

This repo also wires up Databricks' managed MCP server so Claude Code can
query the workspace directly, configured via a project-level `.mcp.json`
rather than Claude's own UI, since the workspace connection is tied to this
project.

## Contents

- [The idea](#the-idea)
- [What is in here](#what-is-in-here)
- [Repo map](#repo-map)
- [Setup](#setup)
- [Design notes](#design-notes)
- [Gotchas worth knowing](#gotchas-worth-knowing)
- [Reference](#reference)

## The idea

Databricks Free Edition restricts outbound internet access from serverless
compute to a small allowlist of trusted domains. Neither BLS nor DataUSA is on
it, so a notebook calling either fails at DNS resolution, not the HTTP layer.
That constraint shapes the whole design: ingestion has to run somewhere that
can reach the network, and everything from there in has to run on Databricks,
deployed as code.

The answer: a fetcher that lives outside Databricks entirely (a GitHub Actions
runner, `sourcing/`), pushing raw bytes and a record of what it did into Unity
Catalog. Two Lakeflow Declarative Pipelines, each deployed via the same
Databricks Asset Bundle, turn those landed files into a bronze, silver, and
gold medallion. The three stages, source and land, bronze, and silver and
gold, are three separate Databricks Jobs, and they are deliberately
decoupled: sourcing triggers nothing downstream, bronze checks the landing
volume for new files on its own schedule rather than being told about them,
and silver and gold checks bronze's tables on its own schedule rather than
being triggered when a bronze run finishes. That decoupling means silver and
gold's schedule can be tuned to what downstream consumers actually need,
faster or slower than ingestion, without touching ingestion at all.

```mermaid
flowchart LR
  BLS["BLS pr/ folder<br/>12 flat files"]:::source
  API["DataUSA population<br/>query endpoint"]:::source
  BLS --> GHA
  API --> GHA
  GHA["Job 1: source and land<br/>GitHub Actions runner<br/><code>sourcing/</code>"]:::job

  GHA -->|Files API| VOL[("landing volume<br/>raw bytes")]:::storage
  GHA -->|Statement Execution API| MAN[("source_manifest<br/>append-only Delta")]:::storage

  VOL -.->|checks on its own schedule| BJ
  BJ["Job 2: bronze / ingest<br/>Auto Loader pipeline"]:::job
  BJ --> BZ[("bronze<br/>11 tables, all STRING")]:::bronze

  BZ -.->|checks on its own schedule| SJ
  SJ["Job 3: silver and gold<br/>declarative pipeline"]:::job
  SJ --> SV[("silver<br/>10 tables, typed + CDC")]:::silver
  SJ --> GD[("gold<br/>3 materialized views")]:::gold
  SV --> GD

  subgraph ENV["dev / stage / prod: one independent copy of everything below"]
    BJ
    SJ
    subgraph UC["Unity Catalog"]
      VOL
      MAN
      BZ
      SV
      GD
    end
  end

  classDef source fill:#6b7280,stroke:#374151,color:#ffffff
  classDef job fill:#2088ff,stroke:#0b5fcc,color:#ffffff
  classDef bronze fill:#cd7f32,stroke:#8b4513,color:#1a1a1a
  classDef silver fill:#c0c0c0,stroke:#5b5b5b,color:#1a1a1a
  classDef gold fill:#ffd700,stroke:#b8860b,color:#1a1a1a
  classDef storage fill:#38bdf8,stroke:#0284c7,color:#ffffff
```

On a paid workspace the network constraint would not exist: serverless has
full outbound access unless a restricted network policy is applied, and the
fetcher would run as a notebook task with no other change. The medallion
split and the decoupled scheduling would stay exactly the same.

## What is in here

**Sources.**

| | BLS productivity | DataUSA population |
|---|---|---|
| Endpoint | [`download.bls.gov/pub/time.series/pr/`](https://download.bls.gov/pub/time.series/pr/) | `honolulu-api.datausa.io` tesseract query |
| Shape | 12 tab-delimited files in a folder | one JSON document per call |
| Update style | files rewritten in place under stable names | full result set returned every call |
| Access control | `403` without a contact-bearing `User-Agent` ([policy](https://www.bls.gov/bls/pss.htm)) | none |
| Change detection | conditional `GET` (`If-Modified-Since`) | `sha256` of the response body |

Filenames are never hardcoded. Every run re-parses the directory listing, so a
file BLS adds or removes is handled without a code change.

**The landing volume**, one independent copy per environment:
`/Volumes/rearc_ingest/<env>/landing/<source>/<dataset>/<filename>__<ingest_ts>`.

```
/Volumes/rearc_ingest/<env>/landing/
├── bls_pr/
│   ├── pr.data.1.AllData/
│   │   ├── pr.data.1.AllData__20260909T163000Z
│   │   └── pr.data.1.AllData__20261105T090200Z
│   ├── pr.series/
│   │   └── pr.series__20260909T163000Z
│   └── …one directory per file in the BLS folder
└── datausa_population/
    └── population/
        └── population.json__20260909T163000Z
```

**The manifest**, `rearc_ingest.<env>.source_manifest`: append-only Delta, one
row per `(source, dataset, ingest_ts)`, written per item on every run,
including unchanged ones.

| Column | Type | |
|---|---|---|
| `source` | STRING | `bls_pr` / `datausa_population` |
| `dataset` | STRING | original filename, or `population` |
| `ingest_ts` | STRING | run stamp, matches the landed filename suffix |
| `status` | STRING | `FETCHED` / `UNCHANGED` / `ERROR` |
| `http_status` | INT | response code, null on a request-level failure |
| `content_sha256` | STRING | hash of the body |
| `last_modified` | STRING | raw header, replayed verbatim as `If-Modified-Since` |
| `bytes` | BIGINT | size landed |
| `source_url` | STRING | exact URL fetched |
| `landing_path` | STRING | where it went, null unless `FETCHED` |
| `run_id` | STRING | GitHub Actions run id, or `local` |
| `fetched_at` | TIMESTAMP | |
| `error_message` | STRING | populated only on `ERROR` |

Each run reads the most recent `FETCHED` row per dataset to build its
conditional headers and hash comparisons. A failed directory listing is
recorded under the sentinel dataset `_directory_listing` rather than failing
silently.

**Environments.** Three environments, one workspace.

| Catalog | Schemas | Holds |
|---|---|---|
| `rearc_ingest` | `dev`, `stage`, `prod` | `landing` volume + `source_manifest`, one independent set per environment |
| `rearc_dev` / `rearc_stage` / `rearc_prod` | `bronze`, `silver`, `gold` | targets for the declarative pipelines |

**Bronze layer**, `resources/dp_bronze_ingestion.yml` + `src/bronze/`: one
Delta table per BLS file, one for DataUSA population, all raw. No filtering,
casting, or validation: every column lands as `STRING`, and every table
carries `_ingested_at` and `_source_file` for provenance. Trimming and typing
are silver's job.

It runs as its own pipeline and its own job (`declarative_bronze_ingestion_job`),
separate from silver and gold. Nothing wires the two together: bronze checks
the landing volume on every run and does nothing when there is nothing new,
the same way sourcing triggers nothing downstream.

**Silver layer**, `resources/dp_silver_gold.yml` (shared with gold) +
`src/silver/`: one table per bronze source, typed and deduplicated. Bronze
lands everything as `STRING`, so silver casts deliberately: `year`, `value`,
`begin_year`, `end_year`, `base_year`, `display_level`, and `sort_sequence`
to numeric types, `selectable` to `BOOLEAN`. Identifier codes (`series_id`,
`measure_code`, `sector_code`, and the rest) stay `STRING`: they are not
quantities, and `measure_code` values like `"01"` would lose their leading
zero under a numeric cast. Deduplication uses SCD Type 1 change-data-capture
(`dp.create_auto_cdc_flow`, sequenced by `_ingested_at`), since both sources
restate history and bronze stacks every snapshot ever landed. `pr_data_0_current`
is left unmodelled: it is a strict subset of `pr_data_1_alldata`, so modelling
it too would only duplicate rows.

**Gold layer**, same pipeline + `src/gold/`: three fully qualified
materialized views. Population summary statistics for 2013 to 2018. Best year
per series, summed over Q01 to Q04 with Q05 (BLS's annual average, not a
fifth quarter) excluded, joined out to a human-readable label built from
`pr_series`'s dimension codes. `PRS30006032`'s Q01 values left-joined with
population, since BLS's history predates and outlives population's coverage
and an inner join would silently drop most of the answer. Each analysis is
implemented twice, once in the PySpark DataFrame API and once in Spark SQL,
with PySpark as the primary that feeds the table and SQL kept alongside as a
documented, runnable alternative.

Silver and gold run as their own pipeline and their own job
(`declarative_silver_gold_job`), decoupled from bronze the same way bronze is
decoupled from sourcing.

## Repo map

```
databricks.yml            bundle definition: env + catalog_prefix, 3 targets
resources/                one file per job or pipeline
sourcing/                 the fetcher, runs on a GitHub runner, not on Databricks
  bls.py                  listing parser + conditional GET
  datausa.py              query endpoint + hash comparison
  landing.py              path construction + Files API upload
  manifest.py             Statement Execution API reads/writes
  config.py               env, catalog_prefix, warehouse resolution
src/bronze/               raw landing tables, one per BLS file + DataUSA
src/silver/               typed, deduplicated tables, one per bronze source
src/gold/                 gold materialized views, PySpark + SQL implementations
src/setup/                catalogs, schemas, volumes, manifest DDL
src/utils/                importable helpers, unit tested
tests/                    bundle guardrails + sourcing unit tests (no Spark)
.github/workflows/        pr, deploy-dev, deploy-stage, deploy-prod, sourcing
```

The fetcher lives outside `src/` on purpose: nothing in `sourcing/` ever runs
on Databricks compute, and nothing it imports is available there.

## Setup

One-time, per workspace:

```bash
databricks auth login --host <workspace-url>
databricks bundle run setup_job --target dev
```

Per code change:

```bash
databricks bundle deploy --target dev
databricks bundle run declarative_bronze_ingestion_job --target dev
databricks bundle run declarative_silver_gold_job --target dev
```

Fetching new source data (needs `BLS_CONTACT_EMAIL` set, and `setup_job` to
have already created the target environment's ingest schema):

```bash
uv run python -m sourcing --env dev
```

Everything above also happens automatically, on a trigger rather than by hand:

| Trigger | Workflow | Effect |
|---|---|---|
| PR to `main` | `pr.yml` | ruff, `bundle validate`, pytest, summary report |
| Merge to `main` | `deploy-dev.yml` | deploy to `dev` |
| Tag `v*-rc*` / dispatch | `deploy-stage.yml` | deploy to `stage` |
| Tag `v*` / dispatch | `deploy-prod.yml` | gated deploy to `prod` |
| Dispatch | `sourcing.yml` | fetch sources into the selected environment |

Trunk-based: a single `main` plus short-lived feature branches. Deploy targets
are bundle targets, not git branches.

## Design notes

**Neither source is append-only:** both restate history, so landing is
immutable and every run that sees new bytes writes a new path rather than
overwriting one.

**Landing is one directory per dataset, not per source.** The 12 BLS files
have 12 different schemas, so each becomes its own table and needs its own
stream. File-first gives every stream a clean, contiguous prefix instead of a
mid-path wildcard.

**The dataset name is the original filename, verbatim.** No normalised key,
so an unknown new file lands correctly with no code change.

**`ingest_ts` is a timestamp, not a date, computed once per run.** Auto
Loader keys its checkpoint on file *path* and will not re-read a path it has
already seen. Since both sources rewrite content under stable names, every
release must land at a path that has never existed. A date would let two
changes on the same day collide and silently lose the second.

**Each environment fetches its own copy** rather than promoting one between
them. That buys cleaner isolation, at the cost of more traffic to the source
and the possibility that two environments hold snapshots taken at different
moments.

**Per item, always download, then upload, then write the manifest row, never
the reverse.**

```mermaid
flowchart TD
  L["List BLS folder"] --> Q{"Seen this file before?"}
  Q -- "no" --> G["GET"]
  Q -- "yes" --> C["GET + If-Modified-Since"]
  C -- "304" --> U["UNCHANGED row"]
  C -- "200" --> G
  G --> UP["Upload to landing volume"]
  UP --> F["FETCHED row"]
  G -. "failure" .-> E["ERROR row"]
```

In that order, every failure degrades to the same bytes landing twice under
two timestamps, which a downstream upsert collapses harmlessly. Reversed, one
failed upload would mean that file is never ingested again and nothing
reports it.

Rows are written per item, not batched, so a crash mid-run leaves resumable
state. One item's failure never aborts the run: the rest still land, and the
process exits non-zero at the end if anything errored. A failed item retries
on the next run by itself, because its stored `last_modified` was never
advanced. There is no rollback and no compensation logic: landing is
immutable, so recovery is a re-run, and an immediate re-run is a no-op that
lands nothing and writes 13 `UNCHANGED` rows.

**Bronze is a separate pipeline and job from silver and gold**, so either can
be redeployed, re-run, or rescheduled independently, the same way sourcing
and bronze are independent. In stage and prod, silver and gold's schedule is
offset an hour after bronze's, since Free Edition only runs one pipeline at a
time, but nothing wires a bronze run finishing to a silver/gold run starting:
the offset is a scheduling courtesy, not a dependency.

**DataUSA lands as one VARIANT column**, not a structured schema. The whole
response is preserved as is via Auto Loader's `singleVariantColumn`, so there
is no `_rescued_data` column on that table: nothing is parsed into fields for
anything to be extra against.

**Every bronze table sets `cluster_by_auto=True`.** Databricks picks
clustering keys from observed query patterns rather than a fixed declaration.

**Bronze lands everything as `STRING` on purpose**
(`cloudFiles.inferColumnTypes` is off), so silver is where real types get
applied deliberately rather than inferred. Every cast was checked against
live data first, not assumed: `selectable` is `T`/`F` only, `display_level`
and `sort_sequence` are plain digits, `begin_year`/`end_year` are always four
digits, and `base_year` uses `-` as BLS's N/A sentinel rather than being
blank.

**Silver dedups with Auto CDC, not a window function.**
`dp.create_auto_cdc_flow`, sequenced by `_ingested_at`, SCD Type 1, is the
platform-native expression of "CDC-applied," and it is what actually
collapses bronze's repeated snapshots to current truth after a restatement.

**Q05 is BLS's annual average, not a fifth quarter.** Gold's "sum of
quarters" logic excludes it explicitly: summing it in inflates every total by
roughly 25% and can flip which year wins.

**Gold implements every analysis twice**, once in the PySpark DataFrame API
and once in Spark SQL, to show fluency in both per the quest's ask. PySpark
is the primary that feeds the table; the SQL version sits next to it as real,
runnable, documented code, not a comment.

## Gotchas worth knowing

- **Delta rejects spaces in column names outright**, regardless of table
  content. BLS pads `series_id` with literal trailing spaces inside the
  header line itself (17 characters fixed width), which fails table creation
  immediately unless `delta.columnMapping.mode = 'name'` is set. This
  surfaced as a real deploy failure, not a hypothetical.
- **BLS returns `403` without a contact-bearing `User-Agent`**, including on
  the directory listing request itself.
- **Free Edition serverless cannot reach either source.** A notebook-based
  fetcher fails at DNS resolution, not the HTTP layer, so the real cause does
  not show up in the error.
- **Auto Loader will not re-read a file path it has already processed.**
  Since BLS and DataUSA both rewrite content under stable filenames, the
  landing path needs a fresh timestamp on every run, or a same-day content
  change is silently lost.
- **A column type change on an existing CDC target needs a full refresh.**
  Delta will not merge `StringType` into `IntegerType` or `BooleanType` in
  place. Deploying silver's numeric casts failed with
  `CANNOT_UPDATE_TABLE_SCHEMA` until the affected tables were full-refreshed.
  Landing is immutable, so the rebuild is deterministic and safe.
- **BLS's header padding is not always trailing.** `series_id` is padded with
  trailing spaces, but `value` is padded with leading spaces, since BLS
  right-justifies numeric columns. Silver strips every column name
  generically rather than assuming one direction.
- **`variant_get` needs a bracket-quoted path for a key with a space.**
  DataUSA's response uses the literal key `"Nation ID"`; the path is
  `$["Nation ID"]`, not `$.Nation ID`.

## Reference

- [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Unity Catalog volumes](https://docs.databricks.com/aws/en/volumes/)
- [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
- [Delta column mapping](https://docs.databricks.com/aws/en/delta/column-mapping)
- [VARIANT type](https://docs.databricks.com/aws/en/semi-structured/variant)
- [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)
- [BLS contact-email policy](https://www.bls.gov/bls/pss.htm)
