from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_sync_workloads_registry_exists():
    text = read("bin/sync-workloads-registry")
    assert "config/workloads_registry.yml" in text
    assert "data_platform/resources/config/workloads_registry.yml" in text
    assert "cp " in text or "cp\n" in text
