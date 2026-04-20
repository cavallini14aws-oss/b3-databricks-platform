from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_operational_contract_exists():
    assert Path("config/official_operational_integration_contract.yml").exists()
    assert Path("data_platform/resources/config/official_operational_integration_contract.yml").exists()

def test_operational_checker_exists():
    text = read("bin/check-operational-integration-contract")
    assert "official_operational_integration_contract.yml" in text
    assert "apply" in text
    assert "smoke" in text

def test_apply_adapter_exists():
    text = read("bin/apply-official-release-adapter")
    assert "stub_apply_success" in text
    assert "release_version" in text
    assert "artifact_wheel" in text

def test_smoke_adapter_exists():
    text = read("bin/smoke-official-release-adapter")
    assert "stub_smoke_success" in text
    assert "checked_at_utc" in text

def test_deploy_protected_uses_adapters():
    text = read("bin/deploy-official-protected")
    assert "apply-official-release-adapter" in text
    assert "smoke-official-release-adapter" in text
