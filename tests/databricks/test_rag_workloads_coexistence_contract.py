from pathlib import Path
import yaml

def test_rag_v1_and_v2_mlflow_exist_in_registry():
    data = yaml.safe_load(Path("config/workloads_registry.yml").read_text(encoding="utf-8"))
    names = {item["name"] for item in data.get("workloads", [])}
    assert "rag_pdf_v1" in names
    assert "rag_pdf_v2_mlflow" in names

def test_rag_v1_and_v2_manifests_exist():
    assert Path(".state/workloads/rag_pdf_v1.yml").exists()
    assert Path(".state/workloads/rag_pdf_v2_mlflow.yml").exists()

def test_rag_v1_and_v2_payloads_exist():
    assert Path(".state/workload-deploy-payloads/rag_pdf_v1-dev.json").exists()
    assert Path(".state/workload-deploy-payloads/rag_pdf_v2_mlflow-dev.json").exists()
