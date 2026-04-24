from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_official_release_clean_exists():
    assert Path("bin/check-official-release-clean").exists()

def test_package_official_release_clean_exists():
    assert Path("bin/package-official-release-clean").exists()

def test_check_official_release_clean_has_forbidden_paths():
    text = read("bin/check-official-release-clean")
    assert ".state/" in text
    assert ".databricks/bundle/" in text
    assert ".local_backup/" in text
    assert ".tmp_env_audit/" in text
    assert "docs/OFFICIAL_INTEGRATION_RUNBOOK.md" in text

def test_package_official_release_clean_uses_git_archive():
    text = read("bin/package-official-release-clean")
    assert "git archive" in text
    assert "zipfile" in text
