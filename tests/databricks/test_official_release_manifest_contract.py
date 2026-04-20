from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_render_official_release_manifest_exists():
    text = read("bin/render-official-release-manifest")
    assert "official-release-manifest" in text
    assert "artifact_wheel" in text
    assert "missing_profiles" in text
    assert ".state/releases" in text

def test_check_official_release_manifest_exists():
    text = read("bin/check-official-release-manifest")
    assert "official-release-manifest-" in text
    assert "Profiles nao prontos" in text
