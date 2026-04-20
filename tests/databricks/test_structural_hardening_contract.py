from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_clean_packaging_exists():
    text = read("bin/check-clean-packaging")
    assert ".env.*.local" in text
    assert ".secrets.*.local" in text
    assert ".state" in text
    assert "dist" in text

def test_check_python_layout_duplicates_exists():
    text = read("bin/check-python-layout-duplicates")
    assert "data_platform/core" in text
    assert "config_loader" in text
    assert "utils" in text
    assert "wrapper explicito" in text

def test_check_config_mirror_integrity_exists():
    text = read("bin/check-config-mirror-integrity")
    assert "workloads_registry.yml" in text
    assert "official_environment_contract.yml" in text
    assert "official_operational_integration_contract.yml" in text
