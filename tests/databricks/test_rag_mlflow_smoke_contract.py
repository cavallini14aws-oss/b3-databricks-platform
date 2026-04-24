import pytest
pytestmark = pytest.mark.heavy

import pytest

from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_rag_mlflow_smoke_contract_exists():
    assert Path("bin/check-rag-mlflow-smoke-contract").exists()

def test_check_rag_mlflow_smoke_contract_has_modes():
    text = read("bin/check-rag-mlflow-smoke-contract")
    assert "soft" in text
    assert "strict" in text
    assert "smoke mlflow exige embedding_endpoint em modo strict" in text
    assert "smoke mlflow exige llm_endpoint em modo strict" in text
    assert "mlflow_enabled" in text
