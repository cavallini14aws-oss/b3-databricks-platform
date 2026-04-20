from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_deploy_conflicts_exists():
    text = read("bin/check-deploy-conflicts")
    assert "release_version" in text
    assert "artifact_wheel" in text
    assert "bundle_targets" in text
    assert "job_count" in text

def test_snapshot_current_release_exists():
    text = read("bin/snapshot-current-release")
    assert "history" in text
    assert "current" in text
    assert "final-official-release" in text
    assert "databricks-deploy-payload" in text

def test_rollback_official_release_exists():
    text = read("bin/rollback-official-release")
    assert "history" in text
    assert "current" in text
    assert "rollback" in text
