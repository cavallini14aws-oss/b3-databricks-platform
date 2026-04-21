from pathlib import Path
import yaml


REQUIRED_TOP_LEVEL_KEYS = {
    "env",
    "workspace",
    "identity",
    "storage",
    "grants",
    "serving",
    "runtime",
    "compute",
    "naming",
    "governance",
}

WORKSPACE_KEYS = {
    "provider",
    "mode",
    "readiness_level",
}

IDENTITY_KEYS = {
    "execution_principal_type",
    "execution_principal_name",
    "execution_group",
    "allowed_executor_types",
    "expected_group",
}

STORAGE_KEYS = {
    "mode",
    "catalog",
    "schema",
    "schema_ai",
    "schema_ops",
    "volume_catalog",
    "volume_schema",
    "volume_name",
    "volume_path",
}

GRANTS_KEYS = {
    "required_table_permissions",
    "required_volume_permissions",
}

SERVING_KEYS = {
    "llm_provider",
    "serving_endpoint",
    "foundation_model_api_allowed",
}

RUNTIME_KEYS = {
    "python_version",
    "dependency_manifest",
    "manual_pip_install_allowed",
}

COMPUTE_KEYS = {
    "access_mode",
    "runtime",
    "unity_catalog_required",
    "allowed_cluster_modes",
    "min_runtime",
    "requires_uc_access",
}

NAMING_KEYS = {
    "expected_catalog",
    "expected_schema",
    "expected_volume_schema",
}

GOVERNANCE_KEYS = {
    "branch_protection_required",
    "pr_merge_gate_required",
    "runtime_guard_required",
}


def load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def assert_readiness_shape(cfg: dict, expected_env: str) -> None:
    assert REQUIRED_TOP_LEVEL_KEYS.issubset(cfg.keys())
    assert cfg["env"] == expected_env

    assert WORKSPACE_KEYS.issubset(cfg["workspace"].keys())
    assert IDENTITY_KEYS.issubset(cfg["identity"].keys())
    assert STORAGE_KEYS.issubset(cfg["storage"].keys())
    assert GRANTS_KEYS.issubset(cfg["grants"].keys())
    assert SERVING_KEYS.issubset(cfg["serving"].keys())
    assert RUNTIME_KEYS.issubset(cfg["runtime"].keys())
    assert COMPUTE_KEYS.issubset(cfg["compute"].keys())
    assert NAMING_KEYS.issubset(cfg["naming"].keys())
    assert GOVERNANCE_KEYS.issubset(cfg["governance"].keys())


def test_official_readiness_dev_contract():
    cfg = load_yaml("config/official_readiness_dev.yml")
    assert_readiness_shape(cfg, "dev")
    assert cfg["governance"]["branch_protection_required"] is False
    assert cfg["governance"]["pr_merge_gate_required"] is False


def test_official_readiness_hml_contract():
    cfg = load_yaml("config/official_readiness_hml.yml")
    assert_readiness_shape(cfg, "hml")
    assert cfg["governance"]["branch_protection_required"] is True
    assert cfg["governance"]["pr_merge_gate_required"] is True


def test_official_readiness_prd_contract():
    cfg = load_yaml("config/official_readiness_prd.yml")
    assert_readiness_shape(cfg, "prd")
    assert cfg["governance"]["branch_protection_required"] is True
    assert cfg["governance"]["pr_merge_gate_required"] is True
