from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_audit_duplicate_module_imports_exists():
    text = read("bin/audit-duplicate-module-imports")
    assert "config_loader" in text
    assert "context" in text
    assert "env" in text
    assert "flags" in text
    assert "logger" in text
    assert "naming" in text
    assert "secrets" in text
    assert "utils" in text

def test_audit_duplicate_module_imports_checks_root_and_core():
    text = read("bin/audit-duplicate-module-imports")
    assert "pattern_root_import" in text
    assert "pattern_core_import" in text
    assert "root_module_imports" in text
    assert "core_module_imports" in text
