from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_dry_run_mentions_real_bundle_steps():
    text = read("bin/dry-run-official-deploy")
    assert "databricks bundle validate" in text
    assert "databricks bundle plan" in text
    assert "databricks bundle summary" in text
    assert "bundle_validate_status" in text
