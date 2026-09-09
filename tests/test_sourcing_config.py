"""Unit tests for sourcing/config.py.

Pure Python: no Spark, no Databricks Connect, no network -- mirrors the
tests/test_bundle_config.py tier so these run in CI in seconds.
"""

import pytest

from sourcing.config import load_run_config, read_catalog_prefix, resolve_warehouse_id


def test_read_catalog_prefix_reads_databricks_yml():
    assert read_catalog_prefix() == "rearc"


class _FakeWarehouse:
    def __init__(self, name, id):
        self.name = name
        self.id = id


class _FakeWarehousesClient:
    def __init__(self, warehouses):
        self._warehouses = warehouses

    def list(self):
        return iter(self._warehouses)


class _FakeClient:
    def __init__(self, warehouses):
        self.warehouses = _FakeWarehousesClient(warehouses)


def test_resolve_warehouse_id_matches_by_name():
    client = _FakeClient(
        [
            _FakeWarehouse("Other Warehouse", "wid-1"),
            _FakeWarehouse("Serverless Starter Warehouse", "wid-2"),
        ]
    )
    assert resolve_warehouse_id(client) == "wid-2"


def test_resolve_warehouse_id_raises_when_absent():
    client = _FakeClient([_FakeWarehouse("Other Warehouse", "wid-1")])
    with pytest.raises(LookupError):
        resolve_warehouse_id(client)


def test_load_run_config_raises_when_contact_email_missing(monkeypatch):
    monkeypatch.delenv("BLS_CONTACT_EMAIL", raising=False)
    with pytest.raises(KeyError):
        load_run_config([])


def test_load_run_config_defaults_env_to_dev(monkeypatch):
    monkeypatch.setenv("BLS_CONTACT_EMAIL", "you@example.com")
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    config = load_run_config([])
    assert config.env == "dev"
    assert config.catalog_prefix == "rearc"
    assert config.run_id == "local"
    assert config.bls_contact_email == "you@example.com"
    assert config.ingest_catalog == "rearc_ingest"
    assert config.manifest_table == "rearc_ingest.dev.source_manifest"
    assert config.landing_base == "/Volumes/rearc_ingest/dev/landing"


def test_load_run_config_reads_env_flag_and_run_id(monkeypatch):
    monkeypatch.setenv("BLS_CONTACT_EMAIL", "you@example.com")
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    config = load_run_config(["--env", "stage"])
    assert config.env == "stage"
    assert config.run_id == "12345"
