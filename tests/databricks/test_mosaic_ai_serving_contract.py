from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_mosaic_ai_serving_contract_exists():
    assert Path("bin/check-mosaic-ai-serving-contract").exists()

def test_check_mosaic_ai_serving_contract_has_modes():
    text = read("bin/check-mosaic-ai-serving-contract")
    assert "soft" in text
    assert "strict" in text
    assert "endpoint_name proibido como TO_BE_DEFINED em modo strict" in text
    assert "route_optimized" in text
    assert "scale_to_zero" in text
