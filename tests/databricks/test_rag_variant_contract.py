import pytest
pytestmark = pytest.mark.heavy

import pytest

from pathlib import Path
import json
import subprocess
import yaml

def ensure_rag_state() -> None:
    needed = [
        Path(".state/workloads/rag_pdf_v1.yml"),
        Path(".state/workloads/rag_pdf_v2_mlflow.yml"),
        Path(".state/workload-deploy-payloads/rag_pdf_v1-dev.json"),
        Path(".state/workload-deploy-payloads/rag_pdf_v2_mlflow-dev.json"),
    ]
    if all(p.exists() for p in needed):
        return

    subprocess.run(
        ["bash", "./bin/bootstrap-rag-workload-state", "dev"],
        check=True,
    )

def test_rag_variants_exist_in_registry():
    data = yaml.safe_load(Path("config/workloads_registry.yml").read_text(encoding="utf-8"))
    workloads = {item["name"]: item for item in data.get("workloads", [])}

    assert workloads["rag_pdf_v1"]["variant"] == "standard"
    assert workloads["rag_pdf_v2_mlflow"]["variant"] == "mlflow"

def test_rag_variants_exist_in_workload_manifests():
    ensure_rag_state()
    v1 = yaml.safe_load(Path(".state/workloads/rag_pdf_v1.yml").read_text(encoding="utf-8"))
    v2 = yaml.safe_load(Path(".state/workloads/rag_pdf_v2_mlflow.yml").read_text(encoding="utf-8"))

    assert v1["variant"] == "standard"
    assert v2["variant"] == "mlflow"

def test_rag_mlflow_variant_adds_mlflow_libraries():
    ensure_rag_state()
    payload = json.loads(Path(".state/workload-deploy-payloads/rag_pdf_v2_mlflow-dev.json").read_text(encoding="utf-8"))
    libs = json.dumps(payload["libraries"], ensure_ascii=False)

    assert payload["variant"] == "mlflow"
    assert "mlflow==3.10.1" in libs

def test_rag_standard_variant_does_not_require_mlflow():
    ensure_rag_state()
    payload = json.loads(Path(".state/workload-deploy-payloads/rag_pdf_v1-dev.json").read_text(encoding="utf-8"))
    libs = json.dumps(payload["libraries"], ensure_ascii=False)

    assert payload["variant"] == "standard"
    assert "mlflow==3.10.1" not in libs
