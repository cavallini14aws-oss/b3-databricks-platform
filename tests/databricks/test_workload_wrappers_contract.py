from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_resolve_workload_profile_exists():
    text = read("bin/resolve-workload-profile")
    assert ".state/workloads/" in text
    assert "profile" in text

def test_render_workload_runtime_spec_exists():
    text = read("bin/render-workload-runtime-spec")
    assert "--workload" in text
    assert "resolve-workload-profile" in text
    assert "render-databricks-runtime-spec" in text

def test_render_workload_job_libraries_exists():
    text = read("bin/render-workload-job-libraries")
    assert "--workload" in text
    assert "resolve-workload-profile" in text
    assert "render-databricks-job-libraries" in text

def test_render_workload_bundle_target_exists():
    text = read("bin/render-workload-bundle-target")
    assert "--workload" in text
    assert "resolve-workload-profile" in text
    assert "render-official-bundle-target" in text

def test_render_workload_deploy_payload_exists():
    text = read("bin/render-workload-deploy-payload")
    assert "workload-deploy-payload" in text
    assert "WORKLOAD_NAME" in text or "workload" in text
    assert ".state/workload-deploy-payloads" in text
