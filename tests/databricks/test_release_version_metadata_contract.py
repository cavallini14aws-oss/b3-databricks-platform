from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_resolve_release_metadata_exists():
    text = read("bin/resolve-release-metadata")
    assert "RELEASE_VERSION=" in text
    assert "GIT_COMMIT=" in text
    assert "GIT_TAG=" in text
    assert "BUILT_AT_UTC=" in text

def test_release_manifest_has_version_metadata():
    text = read("bin/render-official-release-manifest")
    assert "release_version" in text
    assert "git_commit" in text
    assert "git_tag" in text
    assert "built_at_utc" in text

def test_final_release_has_version_metadata():
    text = read("bin/apply-final-official-release")
    assert "release_version" in text
    assert "git_commit" in text
    assert "git_tag" in text

def test_deploy_payload_has_version_metadata():
    text = read("bin/render-databricks-deploy-payload")
    assert "deploy_version" in text
    assert "source_release_version" in text
    assert "git_commit" in text
    assert "git_tag" in text
