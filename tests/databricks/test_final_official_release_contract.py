from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_apply_final_official_release_exists():
    text = read("bin/apply-final-official-release")
    assert "final-official-release" in text
    assert "source_release_manifest" in text
    assert "bundle_target" in text
    assert ".state/releases" in text

def test_show_final_official_release_exists():
    text = read("bin/show-final-official-release")
    assert "final-official-release-" in text
