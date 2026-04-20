from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_official_readiness_by_profile_exists():
    text = read("bin/check-official-readiness-by-profile")
    assert "deploy-plan-" in text
    assert "official-payload-" in text
    assert "runtime-spec-" in text
    assert "job-spec-" in text
    assert "bundle-fragment-" in text
    assert "bundle-target-" in text
