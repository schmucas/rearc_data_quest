# PROCESS

How this was built, and where AI fit.

Architecture, trade-offs and data caveats live in
[README.md](README.md) alongside the diagrams, rather than being restated here.

## Contents

- [AI usage](#ai-usage)
- [Architecture decisions](#architecture-decisions)
- [What would be different for a real client](#what-would-be-different-for-a-real-client)
- [Retrospective](#retrospective)

---

## AI usage

AI was used throughout, in two distinct roles: **Claude Cowork for thinking**
(research, architecture, drafting prompts) and **Claude Code for building**
(implementation, testing, deployment). Keeping those separate is the core of the
workflow below.

### Setup

Started a Claude Cowork project, and created the repo from my own
[Databricks project template](https://github.com/schmucas/databricks_project_template).
The template supplies the DAB definition with three targets driven by `env` and
`catalog_prefix` (so no catalog name is ever hardcoded), four GitHub Actions
workflows covering PR checks and deployment to each environment, a
bronze/silver/gold source layout with unit-tested utilities, an init script, and
the Claude plugin/MCP wiring that pulls in my skills from
[dotclaude](https://github.com/schmucas/dotclaude).

Enabled the Databricks SQL MCP server by adding a `.envrc`, running
`direnv allow`, and launching VS Code with `code .` from the terminal so the MCP
server is invoked at project level with the environment already loaded.

### Understanding the data first

Read the BLS layout documentation at
[`pr.txt`](https://download.bls.gov/pub/time.series/pr/pr.txt) directly, then had
Claude research the rest: the access policy behind the 403, the DataUSA
endpoint's shape. Reading the provider's own docs first is what surfaced 
the `Q05` and unit-heterogeneity issues; neither is visible from the data alone.

### Architecture before implementation

Drafted the whole architecture in Claude Cowork before any code existed:
sourcing strategy, landing layout, manifest design, failure ordering. That
conversation is where the Free Edition egress wall was tested out and the ingest design
moved off-platform.

### Staged delivery

Split the work into five stages: sourcing, bronze, silver + gold, dashboard,
documentation.

### The loop, repeated per stage

1. Draft the Claude Code prompt in Claude Cowork as a markdown file, and
   iterate on it there until it is precise.
2. Hand it to Claude Code in **plan mode on Sonnet at extra-high effort**,
   keeping token usage and context window under
   control before any code is written.
3. Once the plan is agreed: implement, test locally, deploy to `dev`.
4. Validate `dev` twice, independently:
   - **By hand**: the UI for catalog, volumes, tables and pipelines; the SQL
     editor and notebooks for results, with Genie used to draft validation
     queries quickly.
   - **By Claude Code**: via the Databricks CLI, the SQL MCP server, and the
     Databricks AI dev kit.
5. Iterate with Claude Code on gaps until satisfied.
6. Small, self-contained fixes go to a **separate session** rather than the main
   one, to conserve context window and token usage.
7. Fold recurring instructions and design preferences back into my
   [dotclaude](https://github.com/schmucas/dotclaude) skills, and update the
   plugin so the next stage starts with the improved versions.
8. Validate again.
9. Push, deploy all three environments, move to the next stage.

### Where the loop broke down: the dashboard

The stage-by-stage loop above held for everything except the dashboard.

Claude Code built a first version from the prompt, and it was not good enough.
It looked like a dashboard but did not actually answer the quest's three
questions in a way a reviewer could read off it. Several rounds of iteration did
not close the gap.

So I changed approach and built it myself, to my own standards. I authored the
dashboard directly in the workspace UI, reshaping the underlying datasets as I
went, and had Claude Code rework the gold materialized views underneath to match
what the presentation actually needed. Then I pulled the workspace version back
over the local file with a forced `databricks bundle generate dashboard`, pushed
it, adjusted again in the UI, regenerated again, and repeated until it was
finished, then committed the result.

Two things worth taking from that. **The UI is the authoring surface for a
visual artifact and the bundle is the version-control surface**; round-tripping
through `bundle generate` is the honest loop, not hand-authoring Lakeview JSON.
And the split of competence was clear: AI was genuinely useful for the data
work beneath the dashboard, and not a substitute for my own judgement about what
a reader needs to see.

### What AI got wrong, and how it was caught

The pattern that mattered: AI proposed, I verified against primary sources.
Several early design proposals were over-engineered and were cut back
deliberately: a carry-forward scheme in the manifest, an `etag` column and a
`changed` flag all disappeared once filtering to the last successful fetch
proved simpler and more honest. An initial suggestion to couple the fetcher to a
downstream trigger was dropped after the justification for it turned out not to
hold. And a proposal to skip the dual SQL/PySpark implementation was reversed
after re-reading the assignment, which asks for it explicitly.

Every load-bearing claim was checked against the source rather than accepted:
`Q05`'s meaning was confirmed arithmetically from actual rows, the unit ambiguity
behind `value` came from `pr.duration`, and the egress limitation was confirmed
by running the failing call in the workspace rather than trusting a summary.

---

---

## Architecture decisions

- **Landing is immutable.** Neither source appends — both restate history — so
  every changed fetch writes a new timestamped path and nothing is overwritten.
  Re-running is safe by construction: recovery is a re-run, and an immediate
  re-run is a no-op.
- **Idempotency lives in a manifest**, not in file timestamps: conditional `GET`
  for BLS, body hash for DataUSA, one append-only row per item per run.
- **Ordering is the failure design** — download, then upload, then write the row.
  Reversed, a failed upload would mark a file as landed and it would never be
  fetched again.
- **Bronze keeps data exactly as it arrives** — no trimming, casting or
  renaming, everything `STRING`, provenance columns only. That principle is what
  forced Delta column mapping for BLS's space-padded headers.
- **Silver types and deduplicates** with SCD Type 1 auto CDC sequenced by
  `_ingested_at`, because bronze stacks every snapshot ever landed and both
  sources restate history.
- **Gold is SQL-primary; upstream is PySpark.** Gold's audience is BI and analyst
  readers, and a SQL query is what they can read and trust without tracing a
  DataFrame chain. Bronze and silver are PySpark throughout for the opposite
  reason: that is where logic repeats across sources and is worth extracting into
  a shared utils wheel and reusing across projects — something a SQL string
  cannot offer. Both implementations sit in every gold file.
- **Bronze and silver/gold are separate pipelines**, so either can be redeployed,
  re-run and run on different cadence.

Full detail, with diagrams, in [README.md](README.md#the-architecture).

## What would be different for a platform build

- **Catalog-bound workspaces** — bind each catalog to its own workspace so dev
  cannot write prod at all.
- **Ingestion belongs on the data platform, not in CI.** The GitHub Actions
  runner here is a workaround for Free Edition's egress block, not a pattern.
  The cost is not the Python — it is the second CI system, the second secrets
  store, and the second place to look when something breaks.
- **Prefer managed ingestion over a hand-built fetcher.** A Lakeflow Connect
  connector first where one exists, then a third-party tool like Fivetran, and
  a custom fetcher last. BLS is a genuine exception — a directory of flat files
  behind a bot-blocking policy, which no connector covers — but the general
  case rarely justifies the maintenance a hand-built pipeline accrues. Managed
  ingestion is bought for the maintenance and the connector catalog, not to be
  cheaper per row.
- **Access management in a separate infra repo** — Terraform owning grants,
  groups and service principals, reviewed independently of pipeline code.
- **Service principal + OAuth M2M for CI/CD**, not a PAT tied to a personal
  account. Free Edition has no account console, so OAuth M2M isn't available
  and PAT auth in Actions is the workaround here.
- **ABAC on medallion** — fine grained access controll across medallion.
- **PII handling** — classification, masking and row filters, tighter retention.
  Irrelevant for public BLS and Census data; mandatory the moment client data
  lands in the same platform.
- **Shared utils as a versioned wheel in its own repo**, reused across projects,
  instead of a project-local `src/utils/`.
- **Compute policies** — constrain sizing and enforce tagging, so cost is
  attributable before it is a surprise.
- **A cost dashboard** off system tables, split by environment and pipeline, with
  budget alerts.
- **Monitoring** — alerting on expectation failures and on fetch errors, rather
  than a red build nobody is watching.
- **Schema drift needs detection, not just tolerance.** `addNewColumns` means a
  new BLS column lands in bronze, but silver selects explicit columns, so it
  never reaches silver or gold. Nothing breaks — which is exactly the problem:
  the drift is silent. **Genie Zero Ops** would be perfect for this.

## Retrospective

**The sourcing solution is a workaround, not a pattern I would ship.** Running
the fetcher from a GitHub Actions runner gets around the Free Edition egress
restriction, but I would not put a CI runner on the critical path of a
production ingestion. It is sufficient here, and the source cadence makes it
comfortably so: per
[`pr.txt`](https://download.bls.gov/pub/time.series/pr/pr.txt), BLS publishes
this data quarterly (four times a year).

The sharpest edge of that choice is **observability**. The fetcher emits no logs
at all (no prints, no structured logging, and no job summary step), so the
Actions run shows step names and an exit code and nothing about what actually
happened to each of the thirteen items. All of that detail lives in the manifest
table, which means **debugging a failed fetch means leaving GitHub and querying
Databricks**, and a failure early enough to happen *before* the manifest is
reachable (an unset contact address, a bad token, an unresolvable warehouse)
leaves only a raw traceback. On a platform-native ingestion this would come free
from the job's own run history and event log; here it had to be given up, and
the cheap fix (a `GITHUB_STEP_SUMMARY` table of per-item outcomes, which the PR
workflow already does for its checks) is the first thing I would add.

**Bronze keeps the data exactly as it arrives**, which was a deliberate
principle rather than a shortcut: no trimming, no casting, no renaming. That
principle is what forced a specific fix. BLS pads fields to fixed width *inside*
a tab-delimited file, so header names arrive with trailing spaces and Delta
rejects them outright. Rather than rename the columns, the bronze tables enable
Delta column mapping so the raw header lands untouched, and trimming becomes
silver's job.

**I should have read more documentation and trusted Claude less.** The two
issues that would have quietly produced plausible wrong answers (`Q05` being an
annual average, and `value` meaning three different units depending on
`duration_code`) are both plainly documented in the BLS files and invisible in
the data itself. Both were eventually caught, but by verification rather than by
having read carefully in the first place.

**I should have spent longer on the architecture document up front.** A few
nuances got changed well into the build that would have been much cheaper to
settle before any code existed.

**What worked best was iterating on the plan rather than the code.** Time spent
refining what Claude Code produced in plan mode paid back heavily: bugs were
rare, and almost every deploy worked the first time it was pushed.
