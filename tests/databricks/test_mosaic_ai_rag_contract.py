from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_mosaic_ai_rag_contract_exists():
    assert Path("bin/check-mosaic-ai-rag-contract").exists()

def test_check_mosaic_ai_rag_contract_has_modes():
    text = read("bin/check-mosaic-ai-rag-contract")
    assert "soft" in text
    assert "strict" in text
    assert "embedding_endpoint proibido como TO_BE_DEFINED em modo strict" in text
    assert "llm_endpoint proibido como TO_BE_DEFINED em modo strict" in text
    assert "standard_enabled" in text
    assert "mlflow_enabled" in text
