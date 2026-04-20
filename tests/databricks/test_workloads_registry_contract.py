from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_validate_workloads_registry_exists():
    text = read("bin/validate-workloads-registry")
    assert "workloads_registry.yml" in text
    assert "data" in text
    assert "api" in text
    assert "ml" in text
    assert "rag" in text

def test_render_all_workload_manifests_exists():
    text = read("bin/render-all-workload-manifests")
    assert "workload-manifest" in text
    assert ".state/workloads" in text

def test_render_workload_job_spec_exists():
    text = read("bin/render-workload-job-spec")
    assert "workload-job-spec" in text
    assert "WORKLOAD_NAME" in text
    assert "WORKLOAD_ENTRYPOINT" in text
