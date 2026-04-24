import pytest
pytestmark = pytest.mark.heavy

import pytest

from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_bootstrap_rag_workload_state_exists():
    text = read("bin/bootstrap-rag-workload-state")
    assert "render-all-workload-manifests" in text
    assert "render-databricks-job-spec" in text
    assert "render-workload-job-spec" in text
    assert "render-workload-deploy-payload" in text

def test_bootstrap_rag_workload_state_targets_v1_and_v2():
    text = read("bin/bootstrap-rag-workload-state")
    assert "rag_pdf_v1" in text
    assert "rag_pdf_v2_mlflow" in text
