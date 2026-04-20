from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_official_environment_contract_exists():
    assert Path("config/official_environment_contract.yml").exists()
    assert Path("data_platform/resources/config/official_environment_contract.yml").exists()

def test_official_environment_checker_exists():
    text = read("bin/check-official-environment-contract")
    assert "official_environment_contract.yml" in text
    assert "workspace.host" in text
    assert "catalogs.primary" in text
    assert "__FILL_" in text

def test_official_environment_contract_has_expected_sections():
    text = read("config/official_environment_contract.yml")
    assert "workspace:" in text
    assert "catalogs:" in text
    assert "schemas:" in text
    assert "volumes:" in text
    assert "secret_scopes:" in text
    assert "groups:" in text
    assert "cluster_policies:" in text
    assert "service_principals:" in text
