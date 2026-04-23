from pathlib import Path
import yaml

def test_mosaic_ai_contract_exists():
    assert Path("config/mosaic_ai_contract.yml").exists()

def test_mosaic_ai_contract_has_expected_sections():
    data = yaml.safe_load(Path("config/mosaic_ai_contract.yml").read_text(encoding="utf-8"))
    assert "serving" in data
    assert "vector_search" in data
    assert "rag" in data
    assert "monitoring" in data

def test_mosaic_ai_packaged_contract_exists():
    assert Path("data_platform/resources/config/mosaic_ai_contract.yml").exists()
