from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_render_runtime_spec_exists():
    text = read("bin/render-databricks-runtime-spec")
    assert "databricks-runtime-spec" in text
    assert "requirements_compiled" in text
    assert ".state/databricks" in text

def test_render_job_libraries_exists():
    text = read("bin/render-databricks-job-libraries")
    assert "job-libraries" in text
    assert '"whl"' or "'whl'" in text
    assert '"pypi"' or "'pypi'" in text
