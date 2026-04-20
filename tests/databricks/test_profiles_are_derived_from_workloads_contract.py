from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_render_derived_profiles_from_workloads_exists():
    text = read("bin/render-derived-profiles-from-workloads")
    assert "derived-profile-manifest" in text
    assert "derived_from" in text
    assert "config/workloads_registry.yml" in text

def test_render_all_workload_manifests_calls_derived_profiles():
    text = read("bin/render-all-workload-manifests")
    assert "render-derived-profiles-from-workloads" in text
