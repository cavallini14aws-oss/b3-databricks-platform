from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_mosaic_ai_monitoring_contract_exists():
    assert Path("bin/check-mosaic-ai-monitoring-contract").exists()

def test_check_mosaic_ai_monitoring_contract_has_modes():
    text = read("bin/check-mosaic-ai-monitoring-contract")
    assert "soft" in text
    assert "strict" in text
    assert "monitoring exige inference_table em modo strict" in text
    assert "monitoring exige quality_dashboard em modo strict" in text
    assert "monitoring" in text
