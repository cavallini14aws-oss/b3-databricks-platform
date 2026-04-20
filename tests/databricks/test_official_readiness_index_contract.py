from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_render_official_readiness_index_exists():
    text = read("bin/render-official-readiness-index")
    assert "official-readiness-index" in text
    assert '"dev"' or "'dev'" in text
    assert '"hml"' or "'hml'" in text
    assert '"prd"' or "'prd'" in text

def test_show_official_readiness_index_exists():
    text = read("bin/show-official-readiness-index")
    assert ".state/official-readiness-index.json" in text
