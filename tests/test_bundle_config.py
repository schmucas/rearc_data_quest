"""Bundle guardrails.

Pure Python: no Spark session and no Databricks Connect, so this runs on a CI
runner in seconds and every PR gets a real check even before any pipeline logic
exists. These encode the conventions in CLAUDE.md; extend them as the project
grows its own rules.
"""

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
RESOURCES = REPO_ROOT / "resources"

BANNED_PATH_PREFIXES = ("/dbfs/", "dbfs:/", "/mnt/", "dbutils.fs.")


def _bundle() -> dict:
    return yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())


def _source_files():
    for path in SRC.rglob("*"):
        if path.suffix in {".py", ".ipynb"}:
            yield path


def test_every_resource_file_parses():
    files = sorted(RESOURCES.glob("*.yml"))
    assert files, "no resource files found"
    for path in files:
        assert yaml.safe_load(path.read_text()), f"{path.name} parsed empty"


def test_all_three_targets_declare_env():
    targets = _bundle()["targets"]
    assert set(targets) == {"dev", "stage", "prod"}
    for name, target in targets.items():
        assert target["variables"]["env"] == name, f"target {name} declares the wrong env"


def test_catalog_prefix_variable_is_declared():
    assert "catalog_prefix" in _bundle()["variables"]


def test_no_wheel_tasks():
    for path in RESOURCES.glob("*.yml"):
        assert "python_wheel_task" not in path.read_text(), f"{path.name} declares a wheel task"


def test_no_dbfs_paths_in_src():
    for path in _source_files():
        text = path.read_text()
        for prefix in BANNED_PATH_PREFIXES:
            assert prefix not in text, f"{path.relative_to(REPO_ROOT)} references {prefix}"


def test_no_hardcoded_catalog_names_in_src():
    """The catalog is always derived from catalog_prefix + env, never spelled out."""
    prefix = _bundle()["variables"]["catalog_prefix"]["default"]
    hardcoded = re.compile(rf"\b{re.escape(prefix)}_(dev|stage|prod)\b")
    for path in _source_files():
        assert not hardcoded.search(path.read_text()), (
            f"{path.relative_to(REPO_ROOT)} hardcodes a catalog name; derive it from catalog_prefix and env"
        )


def test_pipeline_sources_use_current_declarative_api():
    for path in list((SRC / "silver").rglob("*.py")) + list((SRC / "gold").rglob("*.py")):
        text = path.read_text()
        assert "from pyspark import pipelines as dp" in text, f"{path.name} does not import pyspark.pipelines"
        assert "@dlt." not in text, f"{path.name} uses the legacy dlt spelling"


def test_pipeline_sources_read_config_from_spark_conf():
    """Pipelines have no widgets; env and catalog_prefix come from spark.conf."""
    for path in list((SRC / "silver").rglob("*.py")) + list((SRC / "gold").rglob("*.py")):
        text = path.read_text()
        assert 'spark.conf.get("env")' in text, f"{path.name} does not read env from spark.conf"
        assert 'spark.conf.get("catalog_prefix")' in text, f"{path.name} does not read catalog_prefix from spark.conf"


def test_every_job_passes_env_and_catalog_prefix():
    for path in RESOURCES.glob("*.yml"):
        jobs = (yaml.safe_load(path.read_text()).get("resources") or {}).get("jobs") or {}
        for job_name, job in jobs.items():
            names = {p["name"] for p in job.get("parameters", [])}
            assert {"env", "catalog_prefix"} <= names, f"{path.name}:{job_name} is missing env or catalog_prefix"
