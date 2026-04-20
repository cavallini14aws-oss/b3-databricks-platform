from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_render_databricks_deploy_payload_exists():
    text = read("bin/render-databricks-deploy-payload")
    assert "databricks-deploy-payload" in text
    assert "job_count" in text
    assert "bundle_targets" in text
    assert "source_final_release" in text

def test_show_databricks_deploy_payload_exists():
    text = read("bin/show-databricks-deploy-payload")
    assert "databricks-deploy-payload-" in text
