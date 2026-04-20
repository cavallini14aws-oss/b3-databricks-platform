from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_create_workload_exists():
    text = read("bin/create-workload")
    assert "workload criado" in text or "workload adicionado" in text
    assert "sync-workloads-registry" in text
    assert "render-all-workload-manifests" in text

def test_list_workloads_exists():
    text = read("bin/list-workloads")
    assert "workloads_registry.yml" in text

def test_remove_workload_exists():
    text = read("bin/remove-workload")
    assert "workload removido" in text or "workload removido do registry" in text
    assert "sync-workloads-registry" in text
    assert "render-all-workload-manifests" in text
