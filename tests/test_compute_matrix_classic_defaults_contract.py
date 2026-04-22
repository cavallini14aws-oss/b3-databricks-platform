from pathlib import Path
import yaml


def load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def assert_classic_defaults(cfg: dict) -> None:
    classic = cfg["defaults"]["classic"]
    assert isinstance(classic, dict)
    assert classic["spark_version"]
    assert classic["node_type_id"]
    assert "num_workers" in classic


def test_compute_matrix_main_has_classic_defaults():
    cfg = load_yaml("config/databricks/compute_matrix.yml")
    assert_classic_defaults(cfg)


def test_compute_matrix_resource_mirror_has_classic_defaults():
    cfg = load_yaml("data_platform/resources/config/databricks/compute_matrix.yml")
    assert_classic_defaults(cfg)


def test_compute_matrix_files_are_equal():
    main = Path("config/databricks/compute_matrix.yml").read_text(encoding="utf-8")
    mirror = Path("data_platform/resources/config/databricks/compute_matrix.yml").read_text(encoding="utf-8")
    assert main == mirror
