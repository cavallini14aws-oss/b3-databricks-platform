from pathlib import Path
import subprocess

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

def test_rag_v1_and_v2_manifests_exist():
    ensure_rag_state()
    assert Path(".state/workloads/rag_pdf_v1.yml").exists()
    assert Path(".state/workloads/rag_pdf_v2_mlflow.yml").exists()

def test_rag_v1_and_v2_payloads_exist():
    ensure_rag_state()
    assert Path(".state/workload-deploy-payloads/rag_pdf_v1-dev.json").exists()
    assert Path(".state/workload-deploy-payloads/rag_pdf_v2_mlflow-dev.json").exists()
