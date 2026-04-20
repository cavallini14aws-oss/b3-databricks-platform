from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_render_databricks_job_spec_exists():
    text = read("bin/render-databricks-job-spec")
    assert "databricks-job-spec" in text
    assert "artifact_wheel" in text
    assert "libraries" in text

def test_render_databricks_bundle_fragment_exists():
    text = read("bin/render-databricks-bundle-fragment")
    assert "databricks-bundle-fragment" in text
    assert "resources" in text
    assert "jobs" in text

def test_check_rendered_official_spec_exists():
    text = read("bin/check-rendered-official-spec")
    assert "job-spec-" in text
    assert "bundle-fragment-" in text
    assert "hml" in text
    assert "prd" in text
