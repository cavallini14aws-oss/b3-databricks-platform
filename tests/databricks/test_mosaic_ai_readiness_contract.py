from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_mosaic_ai_readiness_exists():
    assert Path("bin/check-mosaic-ai-readiness").exists()

def test_check_mosaic_ai_readiness_has_modes():
    text = read("bin/check-mosaic-ai-readiness")
    assert "soft" in text
    assert "strict" in text
    assert "placeholder proibido em modo strict" in text
    assert "serving" in text
    assert "vector_search" in text
    assert "monitoring" in text
