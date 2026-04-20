from pathlib import Path
import filecmp

def test_packaged_config_sync():
    src = Path("config")
    dst = Path("data_platform/resources/config")

    assert src.exists(), "config/ ausente"
    assert dst.exists(), "data_platform/resources/config ausente"

    comparison = filecmp.dircmp(src, dst)

    bad = []
    bad.extend(comparison.left_only)
    bad.extend(comparison.right_only)
    bad.extend(comparison.diff_files)

    assert not bad, f"Drift entre config/ e data_platform/resources/config: {bad}"
