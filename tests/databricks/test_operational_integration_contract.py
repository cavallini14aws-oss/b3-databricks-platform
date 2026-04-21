from pathlib import Path
import yaml


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def test_apply_adapter_exists():
    text = read("bin/apply-official-release-adapter")
    assert "databricks bundle validate" in text
    assert "databricks bundle deploy" in text
    assert "apply_success" in text
    assert "stub_apply_success" not in text


def test_smoke_adapter_exists():
    text = read("bin/smoke-official-release-adapter")
    assert "databricks bundle summary" in text
    assert "--force-pull" in text
    assert "databricks bundle run" in text
    assert "BUNDLE_SMOKE_JOB e obrigatorio para" in text
    assert "smoke_assertions.json" in text
    assert "smoke_success" in text
    assert "stub_smoke_success" not in text


def test_operational_contract_mode_updated():
    cfg = load_yaml("config/official_operational_integration_contract.yml")
    res = load_yaml("data_platform/resources/config/official_operational_integration_contract.yml")

    assert cfg["current_mode"] == "databricks_official_cli"
    assert res["current_mode"] == "databricks_official_cli"
    assert cfg == res


def test_operational_contract_smoke_hardening():
    cfg = load_yaml("config/official_operational_integration_contract.yml")
    smoke = cfg["smoke"]

    assert "bundle_profile" in smoke["required_inputs"]
    assert "bundle_deploy_user" in smoke["required_inputs"]
    assert smoke["conditional_required_inputs"]["hml"] == ["bundle_smoke_job"]
    assert smoke["conditional_required_inputs"]["prd"] == ["bundle_smoke_job"]
    assert "smoke_job_required" in smoke["expected_outputs"]
    assert "summary_before_force_pull_log" in smoke["expected_outputs"]
    assert "summary_after_force_pull_log" in smoke["expected_outputs"]
    assert "assertions_json" in smoke["expected_outputs"]
