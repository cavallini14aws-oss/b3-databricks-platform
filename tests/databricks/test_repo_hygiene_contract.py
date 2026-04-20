from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_audit_local_runtime_artifacts_exists():
    text = read("bin/audit-local-runtime-artifacts")
    assert ".pytest_cache" in text
    assert ".state" in text
    assert ".env.*.local" in text

def test_check_clean_packaging_supports_modes():
    text = read("bin/check-clean-packaging")
    assert 'MODE="${1:-workspace}"' in text
    assert "workspace" in text
    assert "review" in text
    assert "package" in text
    assert ".git" in text
    assert ".DS_Store" in text

def test_clean_local_runtime_artifacts_exists():
    text = read("bin/clean-local-runtime-artifacts")
    assert ".pytest_cache" in text
    assert ".state" in text
    assert ".DS_Store" in text

def test_audit_sensitive_local_files_exists():
    text = read("bin/audit-sensitive-local-files")
    assert ".env.*.local" in text
    assert ".secrets.*.local" in text

def test_show_clean_packaging_modes_exists():
    text = read("bin/show-clean-packaging-modes")
    assert "workspace" in text
    assert "review" in text
    assert "package" in text
