from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_release_manifest_supports_variant():
    text = read("bin/render-official-release-manifest")
    assert '"variant"' in text or "'variant'" in text

def test_final_release_supports_variant():
    text = read("bin/apply-final-official-release")
    assert '"variant"' in text or "'variant'" in text

def test_deploy_payload_supports_profile_variants():
    text = read("bin/render-databricks-deploy-payload")
    assert "profile_variants" in text
