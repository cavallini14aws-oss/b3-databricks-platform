from pathlib import Path
import yaml

PROFILE_FILES = [
    Path("manifests/profiles/profile-data.yml"),
    Path("manifests/profiles/profile-api.yml"),
    Path("manifests/profiles/profile-ml.yml"),
    Path("manifests/profiles/profile-rag.yml"),
]

def test_profile_manifests_exist_and_reference_artifact():
    for path in PROFILE_FILES:
        assert path.exists(), f"Manifest ausente: {path}"
        data = yaml.safe_load(path.read_text(encoding="utf-8"))

        assert "profile" in data
        assert "requirements" in data
        assert isinstance(data["requirements"], list)
        assert len(data["requirements"]) >= 1

        assert "artifact" in data
        assert "wheel" in data["artifact"]
        assert str(data["artifact"]["wheel"]).endswith(".whl")

def test_profile_requirements_files_exist():
    for path in PROFILE_FILES:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        for req in data["requirements"]:
            assert Path(req).exists(), f"Requirements ausente no manifest {path}: {req}"
