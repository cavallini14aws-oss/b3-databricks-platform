import pytest
pytestmark = pytest.mark.heavy

import pytest

from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_validate_workloads_registry_checks_entrypoint_existence():
    text = read("bin/validate-workloads-registry")
    assert "entrypoint inexistente" in text
    assert "Path(entrypoint)" in text

def test_registry_entrypoint_files_exist():
    expected = [
        "pipelines/data/clientes_data.py",
        "pipelines/api/clientes_api.py",
        "pipelines/ml/clientes_ml.py",
        "pipelines/rag/clientes_rag.py",
        "pipelines/rag/rag_pdf_v1.py",
        "pipelines/rag/rag_pdf_v2_mlflow.py",
    ]
    for path in expected:
        assert Path(path).exists(), path
