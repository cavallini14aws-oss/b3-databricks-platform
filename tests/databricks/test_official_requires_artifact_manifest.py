from pathlib import Path
import yaml

def test_official_requires_artifact_manifest():
    manifest = Path("manifests/runtime-artifact-manifest.yml")
    latest = Path("manifests/latest-artifact.yml")

    assert manifest.exists(), "runtime-artifact-manifest.yml ausente"
    assert latest.exists(), "latest-artifact.yml ausente"

    manifest_data = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    latest_data = yaml.safe_load(latest.read_text(encoding="utf-8"))

    assert manifest_data["artifact_policy"]["official_envs_require_versioned_artifact"] is True
    assert "artifact" in latest_data
    assert "wheel" in latest_data["artifact"]
