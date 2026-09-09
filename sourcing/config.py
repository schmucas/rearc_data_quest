"""Resolve run configuration for the sourcing fetcher.

This script runs on a GitHub Actions runner, never on Databricks compute, so
there is no dbutils widget and no spark.conf to read `env` / `catalog_prefix`
from (contrast with CLAUDE.md's notebook/pipeline convention). `catalog_prefix`
is instead parsed straight out of databricks.yml's variable default -- the same
source of truth the bundle itself uses, and the same `yaml.safe_load` pattern
tests/test_bundle_config.py already relies on -- so it can never drift into a
second, independently hardcoded value. `env` has no single meaningful default
in databricks.yml (every target sets it explicitly), so it comes from a CLI
flag instead, defaulting to "dev" since this fetcher only ever targets dev
today.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient

REPO_ROOT = Path(__file__).resolve().parents[1]
DATABRICKS_YML = REPO_ROOT / "databricks.yml"
WAREHOUSE_NAME = "Serverless Starter Warehouse"


@dataclass(frozen=True)
class RunConfig:
    """Resolved configuration for one fetcher run.

    Attributes:
        env: Deployment environment, e.g. "dev".
        catalog_prefix: Unity Catalog naming root, e.g. "rearc".
        run_id: GitHub Actions run id (GITHUB_RUN_ID), or "local" off CI.
        bls_contact_email: Contact address BLS requires in the User-Agent header.
    """

    env: str
    catalog_prefix: str
    run_id: str
    bls_contact_email: str

    @property
    def ingest_catalog(self) -> str:
        """The shared ingest catalog, e.g. 'rearc_ingest'."""
        return f"{self.catalog_prefix}_ingest"

    @property
    def manifest_table(self) -> str:
        """Fully qualified source_manifest table name."""
        return f"{self.ingest_catalog}.{self.env}.source_manifest"

    @property
    def landing_base(self) -> str:
        """Root of the landing volume for this environment."""
        return f"/Volumes/{self.ingest_catalog}/{self.env}/landing"


def read_catalog_prefix(databricks_yml: Path = DATABRICKS_YML) -> str:
    """Read catalog_prefix's default straight out of databricks.yml.

    Args:
        databricks_yml: Path to the bundle definition file.

    Returns:
        The `catalog_prefix` variable's default value.
    """
    bundle = yaml.safe_load(databricks_yml.read_text())
    return bundle["variables"]["catalog_prefix"]["default"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the fetcher entry point.

    Args:
        argv: Argument list, or None to read from sys.argv.

    Returns:
        Namespace with `env`.
    """
    parser = argparse.ArgumentParser(description="Fetch BLS + DataUSA sources into the landing volume.")
    parser.add_argument("--env", default="dev", help="Target environment (default: dev).")
    return parser.parse_args(argv)


def load_run_config(argv: list[str] | None = None) -> RunConfig:
    """Assemble a RunConfig from CLI args, env vars, and databricks.yml.

    Args:
        argv: Argument list, or None to read from sys.argv.

    Returns:
        A fully resolved RunConfig.

    Raises:
        KeyError: If BLS_CONTACT_EMAIL is unset -- fail fast rather than send
            BLS an unattributed request and get a silent 403.
    """
    args = parse_args(argv)
    return RunConfig(
        env=args.env,
        catalog_prefix=read_catalog_prefix(),
        run_id=os.environ.get("GITHUB_RUN_ID", "local"),
        bls_contact_email=os.environ["BLS_CONTACT_EMAIL"],
    )


def resolve_warehouse_id(client: WorkspaceClient, name: str = WAREHOUSE_NAME) -> str:
    """Resolve a SQL warehouse id by display name.

    Args:
        client: Authenticated WorkspaceClient (auth from DATABRICKS_HOST / DATABRICKS_TOKEN).
        name: The warehouse's display name, matching databricks.yml's lookup.

    Returns:
        The warehouse id.

    Raises:
        LookupError: If no warehouse with that name exists in the workspace.
    """
    for warehouse in client.warehouses.list():
        if warehouse.name == name:
            return warehouse.id
    raise LookupError(f"no SQL warehouse named {name!r} found in this workspace")
