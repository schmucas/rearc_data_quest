# `sourcing/` — the source fetcher

Pulls the full contents of the BLS productivity folder and the DataUSA
population endpoint into a Unity Catalog volume, and records every fetch
attempt in an append-only Delta manifest.

Runs on a GitHub Actions runner, never on Databricks compute.

## Contents

- [One run, end to end](#one-run-end-to-end)
- [Module map](#module-map)
- [Getting past BLS's bot blocking](#getting-past-blss-bot-blocking)
- [Not hammering the source](#not-hammering-the-source)
- [Why it is not scheduled](#why-it-is-not-scheduled)
- [Never hardcoding filenames](#never-hardcoding-filenames)
- [Change detection](#change-detection)
- [Added, changed, removed](#added-changed-removed)
- [Where files land](#where-files-land)
- [The manifest](#the-manifest)
- [Failure model](#failure-model)
- [Configuration](#configuration)
- [Databricks APIs used](#databricks-apis-used)
- [Tests](#tests)
- [Known gaps](#known-gaps)

## One run, end to end

```
resolve config  →  read last known state from the manifest
                →  GET the BLS directory listing
                →  for each file, serially:
                       conditional GET
                       304 → write an UNCHANGED row, stop
                       200 → upload to the volume, then write a FETCHED row
                →  GET the DataUSA endpoint
                       hash matches → UNCHANGED row
                       hash differs → upload, then FETCHED row
                →  exit non-zero if anything errored
```

A steady-state run writes 13 manifest rows (12 BLS files plus the DataUSA
document) and transfers almost nothing, since every BLS file answers `304`.

## Module map

| File | Responsibility |
|---|---|
| `__main__.py` | Orchestration: one fetch cycle, per-item processing, exit code |
| `bls.py` | Directory-listing parser, contact `User-Agent` session, conditional `GET` |
| `datausa.py` | Query endpoint fetch and `sha256` comparison |
| `landing.py` | Landing-path construction and Files API upload |
| `manifest.py` | Statement Execution API reads and writes |
| `config.py` | `env`, `catalog_prefix`, contact address, warehouse resolution |
| `models.py` | `FetchOutcome`, the shape passed from fetchers to the orchestrator |

Entry point: `uv run python -m sourcing --env dev`.

## Getting past BLS's bot blocking

- BLS returns `403 Forbidden` to any request without a `User-Agent` carrying
  contact information. Their
  [data access policy](https://www.bls.gov/bls/pss.htm) reserves the right to
  block robots with no way to contact the owner, so this is a policy
  requirement, not an obstacle to route around.
- `build_session()` sets the header once on a `requests.Session`:
  ```
  User-Agent: rearc_data_quest data pipeline (<contact address>)
  ```
  Setting it on the session, not per call, means **every** request carries it,
  including the directory-listing `GET` itself, the one most likely to be
  forgotten and the one that fails first.
- The address comes from the `BLS_CONTACT_EMAIL` environment variable.
  `load_run_config()` reads it with `os.environ[...]` rather than `.get()`, so
  an unset address fails the run immediately instead of sending BLS an
  unattributed request and getting a `403` that looks like something else.
- **Local development:** set `BLS_CONTACT_EMAIL` in `.envrc` (see
  `.envrc.example`). `.envrc` is gitignored, so it never lands in the repo.

## Not hammering the source

BLS is a public government server being asked for a favour. Four things keep
the load negligible:

- **Conditional requests.** A no-op run transfers response headers and no
  bodies. The full payload is ~4.9 MB, moved only when BLS republishes.
- **Serial, never parallel.** The twelve files fetch one after another, no
  concurrency to tune, no burst of simultaneous connections.
- **Explicit timeouts.** Every request uses a 60-second timeout, so a hanging
  connection fails rather than holding a socket open indefinitely.
- **Run cadence matched to publication cadence.** BLS publishes quarterly, and
  the workflow does not poll on a timer. See below.

The honest limitation: **no retry, backoff, or `Retry-After` handling**. A
transient failure becomes an `ERROR` row and a red run instead of being
retried in place. That's safe, since the item retries on the next run by
itself (its stored `last_modified` was never advanced), but it's the first
thing to add for production use.

## Why it is not scheduled

The workflow is `workflow_dispatch` only. A `schedule:` block would be three
lines, and it's deliberately left out because this is a demo repo, not a
running service:

- **The source changes four times a year.** A nightly cron would issue ~360
  no-op runs annually to catch four real ones, each writing 13 manifest rows
  regardless, filling the audit table with evidence of nothing happening.
- **A reviewer should be able to trigger it and watch it.** On-demand runs
  with an environment selector demonstrate the thing working; a cron firing
  into a repo under review is background noise in the Actions tab.
- **Only `dev` has been through `setup_job`.** A schedule targeting `stage` or
  `prod` would fail until those environments' ingest schema and manifest
  table exist.

For a real deployment: one `schedule:` block plus retry handling, cadenced to
BLS's quarterly release, not a daily timer.

## Never hardcoding filenames

- Every run re-parses the directory listing. No filename appears anywhere in
  the module.
- BLS serves an IIS-style listing, not Apache's `mod_autoindex`, so hrefs are
  **absolute paths** (`/pub/time.series/pr/pr.class`), not bare relative
  filenames. `_DirectoryListingParser` keeps only the basename, so it's
  agnostic to either style.
- Links are excluded when the href ends in `/` (the `[To Parent Directory]`
  link and any subdirectory) or starts with `?` (sort-order query links).
- Only filenames are parsed. The listing's dates and sizes are deliberately
  ignored: the `Last-Modified` response header is authoritative, and depending
  on the HTML's date format would be fragility for no gain.

## Change detection

The two sources need different mechanisms, since they're different kinds of
thing:

| | BLS | DataUSA |
|---|---|---|
| Shape | 12 files in a folder | one query endpoint |
| Validators | sends `Last-Modified` | none — no `ETag`, no `Last-Modified` |
| Mechanism | conditional `GET` with `If-Modified-Since` | `sha256` of the response body |
| "Unchanged" looks like | HTTP `304`, empty body | `200` whose body hashes to the stored value |

The stored `last_modified` is replayed **verbatim**, never reparsed or
reformatted. Rewriting an HTTP date can easily produce a header the server
doesn't recognise, and the failure mode is silent: `200` every time, and the
same bytes land forever.

## Added, changed, removed

| Source-side event | What happens |
|---|---|
| **File added** | Appears in the listing with no prior manifest row, so no conditional header is sent and it's always fetched. Landing directory created on first upload. No code change needed |
| **File changed** | `200` with a new body: uploaded to a fresh path, `FETCHED` row written with the new `last_modified` |
| **File unchanged** | `304`: nothing uploaded, `UNCHANGED` row written as proof the check happened |
| **File removed** | Drops out of the listing, so no new rows are written. Manifest history and previously landed snapshots both remain, nothing is deleted, nothing downstream breaks |

Removal is handled by not breaking rather than by writing a tombstone: a
deliberate consequence of an immutable landing zone, where what was true at
the time it was fetched stays recorded.

## Where files land

```
/Volumes/<catalog_prefix>_ingest/<env>/landing/<source>/<dataset>/<filename>__<ingest_ts>
```

```
landing/bls_pr/pr.series/pr.series__20260909T163000Z
landing/bls_pr/pr.data.1.AllData/pr.data.1.AllData__20260909T163000Z
landing/datausa_population/population/population.json__20260909T163000Z
```

Three decisions in that shape:

- **One directory per dataset.** The 12 BLS files have 12 different schemas,
  so each becomes its own bronze table with its own Auto Loader stream. A
  directory per dataset gives every stream a clean, contiguous prefix.
- **The dataset segment is the original filename, verbatim**, dots included.
  No normalised key, so an unknown new file lands correctly with no code
  change.
- **`ingest_ts` is a timestamp, not a date, computed once per run**
  (`new_ingest_ts()`). Auto Loader keys its checkpoint on file *path* and
  won't re-read a path it has already processed. Both sources rewrite content
  under stable names, so every republication has to land somewhere Auto
  Loader has never seen. A date would let two changes on the same UTC day
  collide, and the second would be silently lost.

`upload()` calls `create_directory` on the parent before writing, since a new
dataset's directory doesn't exist on its first run.

## The manifest

`<catalog_prefix>_ingest.<env>.source_manifest`: Delta,
`delta.appendOnly = true`, DDL owned by `src/setup/environment_setup.ipynb`.

One row per `(source, dataset, ingest_ts)`, per item, per run, including
unchanged ones, since "checked, nothing had changed" is an audit statement
worth being able to make.

Reading back the last known state filters to successful fetches only:

```sql
WHERE source = :source AND status = 'FETCHED'
QUALIFY ROW_NUMBER() OVER (PARTITION BY dataset ORDER BY ingest_ts DESC) = 1
```

`UNCHANGED` and `ERROR` rows leave `last_modified`, `content_sha256` and
`landing_path` null, so every row is an honest record of one attempt rather
than carrying values forward from an earlier one.

All values travel as **named SQL parameters** (`:name`), never interpolated
into statement text.

## Failure model

Per item, always:

```
download  →  upload to the volume  →  write the manifest row
```

Never the reverse. In that order, every failure degrades to the same bytes
landing twice under two timestamps, which the silver upsert collapses
harmlessly. Reversed, a failed upload leaves a row claiming the file is
landed, and it's never fetched again: silent, permanent, unreported.

- **Rows are written per item, not batched**, so a crash after five of twelve
  leaves resumable state rather than a lie.
- **One item's failure never aborts the run.** The remaining files still
  fetch and land; the process exits non-zero at the end if anything errored.
- **A failed directory listing** is recorded under the sentinel dataset
  `_directory_listing`, so a run that fetched nothing is distinguishable from
  a run that was never attempted.
- **No rollback, no compensation logic.** Landing is immutable, so recovery
  is a re-run, and an immediate re-run is a no-op that lands nothing.

## Configuration

| Input | Source |
|---|---|
| `env` | `--env` CLI flag, default `dev` |
| `catalog_prefix` | parsed from `databricks.yml`'s variable default |
| `BLS_CONTACT_EMAIL` | environment variable; required, no fallback |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | environment, read by the SDK |
| `GITHUB_RUN_ID` | environment; recorded as `run_id`, or `local` off CI |
| SQL warehouse | resolved by display name, matching `databricks.yml`'s lookup |

`catalog_prefix` is read from `databricks.yml` rather than duplicated, so it
can't drift from what the bundle deploys. `env` has no meaningful default
there (every target sets it explicitly), so it comes from the flag instead.

Locally, `BLS_CONTACT_EMAIL` and the Databricks credentials are set in
`.envrc` (gitignored); see `.envrc.example` for the template.

## Databricks APIs used

Both through `databricks-sdk`'s `WorkspaceClient`, which picks up
`DATABRICKS_HOST` / `DATABRICKS_TOKEN` from the environment.

- **Files API**: `create_directory` then `upload` for each landed file. Bytes
  are written exactly as received, with no re-encoding.
- **SQL Statement Execution API**: manifest reads and writes. Statements are
  submitted with a 30-second synchronous wait and `ON_WAIT_TIMEOUT=CONTINUE`,
  then polled to a 120-second ceiling, covering a cold Free Edition
  serverless warehouse taking longer than the API's own 50-second
  synchronous cap.

## Tests

`tests/test_sourcing_*.py`: pure Python, no network, no Spark, no Databricks
Connect, so they run in seconds in CI alongside the bundle guardrails. They
cover the listing parser (including parent-directory exclusion and
absolute-href basenames), conditional-GET outcomes, hash comparison, landing
path construction, manifest row shaping, and orchestration control flow.

## Known gaps

- No retry, backoff, or `Retry-After` handling on transient failures.
- No post-upload size verification against `content_length`.
