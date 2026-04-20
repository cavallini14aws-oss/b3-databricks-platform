from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_deploy_official_protected_exists():
    text = read("bin/deploy-official-protected")
    assert "check-deploy-conflicts" in text
    assert "snapshot-current-release" in text
    assert "rollback-official-release" in text
    assert "smoke" in text

def test_deploy_official_protected_has_rollback_path():
    text = read("bin/deploy-official-protected")
    assert "rollback_and_reapply" in text
    assert "smoke falhou" in text or "apply falhou" in text
