from pathlib import Path
from types import SimpleNamespace

from data_platform.aiops.retrieval.storage_guard import validate_ai_storage_target


def test_validate_ai_storage_target_path_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "data_platform.aiops.retrieval.storage_guard.load_yaml_config",
        lambda path: {
            "storage": {
                "mode": "path",
                "base_path": str(tmp_path / "ai_local"),
            }
        },
    )

    monkeypatch.setattr(
        "data_platform.aiops.retrieval.storage_guard.get_context",
        lambda **kwargs: SimpleNamespace(naming=SimpleNamespace()),
    )

    result = validate_ai_storage_target(
        spark=None,
        project="clientes",
        config_path="dummy.yml",
        use_catalog=False,
    )

    assert result["status"] == "ALLOW"
    assert result["storage_mode"] == "path"
    assert Path(result["target"]).exists()
