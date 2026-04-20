from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_dry_run_official_deploy_exists():
    text = read("bin/dry-run-official-deploy")
    assert "official-release-manifest" in text
    assert "final-official-release" in text
    assert "databricks-deploy-payload" in text

def test_dry_run_official_deploy_validates_core_fields():
    text = read("bin/dry-run-official-deploy")
    assert "release_version" in text
    assert "artifact_wheel" in text
    assert "job_count" in text
    assert "profile_variants" in text

def test_dry_run_official_deploy_checks_readiness():
    text = read("bin/dry-run-official-deploy")
    assert "ready" in text
