from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_apply_official_profile_exists():
    text = read("bin/apply-official-profile")
    assert "official-apply-payload" in text
    assert "requirements_compiled" in text
    assert ".state/official" in text

def test_check_official_profile_gate_exists():
    text = read("bin/check-official-profile-gate")
    assert "deploy-plan-" in text
    assert "hml" in text
    assert "prd" in text
