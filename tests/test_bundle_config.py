"""Bundle guardrails.

Pure Python: no Spark session and no Databricks Connect, so this runs on a CI
runner in seconds and every PR gets a real check even before any pipeline logic
exists. These encode the conventions in CLAUDE.md; extend them as the project
grows its own rules.
"""

import json
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
RESOURCES = REPO_ROOT / "resources"
DASHBOARD_RESOURCE = RESOURCES / "dashboard.yml"

BANNED_PATH_PREFIXES = ("/dbfs/", "dbfs:/", "/mnt/", "dbutils.fs.")


def _bundle() -> dict:
    return yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())


def _dashboard_json() -> tuple[dict, Path]:
    resource = yaml.safe_load(DASHBOARD_RESOURCE.read_text())
    file_path = resource["resources"]["dashboards"]["rearc_gold_dashboard"]["file_path"]
    resolved = (RESOURCES / file_path).resolve()
    return json.loads(resolved.read_text()), resolved


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


def _pipeline_source_files():
    return (
        list((SRC / "bronze").rglob("*.py")) + list((SRC / "silver").rglob("*.py")) + list((SRC / "gold").rglob("*.py"))
    )


def test_pipeline_sources_use_current_declarative_api():
    for path in _pipeline_source_files():
        text = path.read_text()
        assert "from pyspark import pipelines as dp" in text, f"{path.name} does not import pyspark.pipelines"
        assert "@dlt." not in text, f"{path.name} uses the legacy dlt spelling"


def test_pipeline_sources_read_config_from_spark_conf():
    """Pipelines have no widgets; env and catalog_prefix come from spark.conf."""
    for path in _pipeline_source_files():
        text = path.read_text()
        assert 'spark.conf.get("env")' in text, f"{path.name} does not read env from spark.conf"
        assert 'spark.conf.get("catalog_prefix")' in text, f"{path.name} does not read catalog_prefix from spark.conf"


def test_every_job_passes_env_and_catalog_prefix():
    for path in RESOURCES.glob("*.yml"):
        jobs = (yaml.safe_load(path.read_text()).get("resources") or {}).get("jobs") or {}
        for job_name, job in jobs.items():
            names = {p["name"] for p in job.get("parameters", [])}
            assert {"env", "catalog_prefix"} <= names, f"{path.name}:{job_name} is missing env or catalog_prefix"


def _pipeline_includes_glob(glob: str) -> bool:
    for path in RESOURCES.glob("*.yml"):
        pipelines = (yaml.safe_load(path.read_text()).get("resources") or {}).get("pipelines") or {}
        for pipeline in pipelines.values():
            includes = {lib["glob"]["include"] for lib in pipeline.get("libraries", [])}
            if glob in includes:
                return True
    return False


def test_bronze_included_in_pipeline_libraries():
    """Some declarative pipeline must actually load the bronze sources."""
    assert _pipeline_includes_glob("../src/bronze/**"), "no pipeline includes ../src/bronze/**"


def test_silver_included_in_pipeline_libraries():
    """Some declarative pipeline must actually load the silver sources."""
    assert _pipeline_includes_glob("../src/silver/**"), "no pipeline includes ../src/silver/**"


def test_gold_included_in_pipeline_libraries():
    """Some declarative pipeline must actually load the gold sources."""
    assert _pipeline_includes_glob("../src/gold/**"), "no pipeline includes ../src/gold/**"


def test_pipeline_sources_avoid_bare_dlt_word():
    """Stricter than the existing '@dlt.' substring check: no bare `dlt` word at all."""
    bare_dlt = re.compile(r"\bdlt\b")
    for path in _pipeline_source_files():
        text = path.read_text()
        assert not bare_dlt.search(text), f"{path.name} references the legacy dlt spelling"


def test_gold_table_names_fully_qualified():
    """Gold names are fully qualified; only silver is bare."""
    for path in (SRC / "gold").rglob("*.py"):
        text = path.read_text()
        assert "{CATALOG}.gold." in text, f"{path.name} does not fully qualify its gold table name"


def test_silver_table_names_are_bare():
    """Silver dataset names are bare -- the pipeline's default schema is silver."""
    for path in (SRC / "silver").rglob("*.py"):
        text = path.read_text()
        assert "{CATALOG}.silver." not in text, f"{path.name} qualifies a silver name; silver names must be bare"
        assert "{CATALOG}.gold." not in text, f"{path.name} references a gold-qualified name from silver"


def test_no_spark_sql_in_silver():
    """Silver is PySpark-only transformation plumbing -- no SQL strings. Gold is exempt."""
    for path in (SRC / "silver").rglob("*.py"):
        assert "spark.sql(" not in path.read_text(), f"{path.name} uses spark.sql(; silver must be PySpark only"


def test_bronze_never_sets_checkpoint_or_schema_location():
    """Auto Loader checkpoint/schema tracking is pipeline-managed; never set explicitly."""
    for path in (SRC / "bronze").rglob("*.py"):
        text = path.read_text()
        assert "checkpointLocation" not in text, f"{path.name} sets checkpointLocation"
        assert "schemaLocation" not in text, f"{path.name} sets schemaLocation"


def test_dashboard_resource_declares_dashboards():
    resource = yaml.safe_load(DASHBOARD_RESOURCE.read_text())
    assert "dashboards" in resource.get("resources", {}), "dashboard.yml declares no dashboards resource"


def test_dashboard_file_path_resolves():
    resource = yaml.safe_load(DASHBOARD_RESOURCE.read_text())
    file_path = resource["resources"]["dashboards"]["rearc_gold_dashboard"]["file_path"]
    resolved = (RESOURCES / file_path).resolve()
    assert resolved.is_file(), f"dashboard file_path does not resolve to a file: {resolved}"


def test_dashboard_json_has_datasets_and_pages():
    dashboard, _ = _dashboard_json()
    assert dashboard.get("datasets"), "dashboard JSON has no datasets"
    assert dashboard.get("pages"), "dashboard JSON has no pages"


def test_dashboard_widget_dataset_names_exist():
    dashboard, _ = _dashboard_json()
    dataset_names = {d["name"] for d in dashboard["datasets"]}
    for page in dashboard["pages"]:
        for entry in page.get("layout", []):
            for query in entry["widget"].get("queries", []):
                name = query["query"]["datasetName"]
                assert name in dataset_names, f"widget references unknown datasetName {name!r}"


def test_dashboard_json_has_no_hardcoded_catalog_names():
    prefix = _bundle()["variables"]["catalog_prefix"]["default"]
    hardcoded = re.compile(rf"\b{re.escape(prefix)}_(dev|stage|prod)\b")
    _, path = _dashboard_json()
    assert not hardcoded.search(path.read_text()), f"{path.name} hardcodes a catalog name"


def test_dashboard_references_both_gold_views():
    dashboard, _ = _dashboard_json()
    all_sql = " ".join("".join(d.get("queryLines", [])) for d in dashboard["datasets"])
    for view in ("population_stats", "agg_value_per_year"):
        assert view in all_sql, f"no dataset queries {view}"
