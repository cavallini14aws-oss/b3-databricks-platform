from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_sensitive_policy_doc_exists():
    assert Path("docs/LOCAL_SENSITIVE_FILES_POLICY.md").exists()

def test_sensitive_policy_checker_exists():
    text = read("bin/check-sensitive-local-files-policy")
    assert "LOCAL_SENSITIVE_FILES_POLICY.md" in text
    assert "audit-sensitive-local-files" in text
    assert ".example" in text

def test_create_local_sensitive_templates_exists():
    text = read("bin/create-local-sensitive-templates")
    assert ".env.*.local" in text
    assert ".secrets.*.local" in text
    assert ".example" in text
