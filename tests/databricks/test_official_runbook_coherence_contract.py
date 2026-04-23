from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_official_runbook_coherence_exists():
    assert Path("bin/check-official-runbook-coherence").exists()

def test_runbook_coherence_check_has_required_refs():
    text = read("bin/check-official-runbook-coherence")
    assert "./bin/check-official-release-clean" in text
    assert "./bin/package-official-release-clean" in text
    assert "./bin/dry-run-official-deploy" in text
    assert "./bin/rollback-official-release" in text
    assert "config/official_environment_contract.yml" in text
