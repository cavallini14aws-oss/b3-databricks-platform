from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_resolve_runtime_profile_exists():
    text = read("bin/resolve-runtime-profile")
    assert "profile-" in text
    assert "manifests/profiles/" in text

def test_validate_runtime_profile_exists():
    text = read("bin/validate-runtime-profile")
    assert "artifact.wheel" in text or "Wheel ausente" in text
    assert "Requirements ausente" in text or "requirements" in text

def test_deploy_official_profile_contract():
    text = read("bin/deploy-official-profile")
    assert "--env" in text
    assert "--profile" in text
    assert "official-deploy-plan" in text
    assert ".state" in text
