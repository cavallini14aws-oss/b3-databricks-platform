from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_rag_standard_smoke_contract_exists():
    assert Path("bin/check-rag-standard-smoke-contract").exists()

def test_check_rag_standard_smoke_contract_has_modes():
    text = read("bin/check-rag-standard-smoke-contract")
    assert "soft" in text
    assert "strict" in text
    assert "smoke standard exige embedding_endpoint em modo strict" in text
    assert "smoke standard exige llm_endpoint em modo strict" in text
    assert "standard_enabled" in text
