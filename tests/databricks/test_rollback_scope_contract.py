from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_rollback_explicitly_marks_scope():
    text = read("bin/rollback-official-release")
    assert "rollback_scope" in text
    assert "declarative_restore_plus_optional_reapply_smoke" in text
    assert "DATABRICKS_ROLLBACK_REAPPLY" in text
