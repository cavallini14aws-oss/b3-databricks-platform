from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_render_official_bundle_target_exists():
    text = read("bin/render-official-bundle-target")
    assert "official-bundle-target" in text
    assert "bundle_fragment_source" in text
    assert "resources" in text

def test_check_official_bundle_target_exists():
    text = read("bin/check-official-bundle-target")
    assert "bundle-target-" in text
    assert "hml" in text
    assert "prd" in text
