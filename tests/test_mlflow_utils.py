import pytest

from data_platform.mlops.mlflow_utils import (
    _get_mlflow_module,
    build_mlflow_experiment_path,
    set_mlflow_experiment_for_project,
)


def test_build_mlflow_experiment_path_for_train():
    result = build_mlflow_experiment_path(project="clientes", stage="train")
    assert result == "/Shared/mlops/clientes/train"


def test_build_mlflow_experiment_path_for_evaluate():
    result = build_mlflow_experiment_path(project="clientes", stage="evaluate")
    assert result == "/Shared/mlops/clientes/evaluate"


def test_build_mlflow_experiment_path_blocks_invalid_stage():
    with pytest.raises(ValueError, match="MLflow stage invalido"):
        build_mlflow_experiment_path(project="clientes", stage="deploy")


def test_get_mlflow_module_raises_runtime_error_when_missing(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "mlflow":
            raise ModuleNotFoundError("No module named 'mlflow'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="mlflow nao esta instalado"):
        _get_mlflow_module()


def test_set_mlflow_experiment_for_project_uses_expected_path(monkeypatch):
    captured = {"path": None}

    class DummyMlflow:
        @staticmethod
        def set_experiment(experiment_path):
            captured["path"] = experiment_path

    monkeypatch.setattr(
        "data_platform.mlops.mlflow_utils._ensure_workspace_parent_path",
        lambda experiment_path: None,
    )
    monkeypatch.setattr(
        "data_platform.mlops.mlflow_utils._get_mlflow_module",
        lambda: DummyMlflow(),
    )

    result = set_mlflow_experiment_for_project(project="clientes", stage="train")

    assert result == "/Shared/mlops/clientes/train"
    assert captured["path"] == "/Shared/mlops/clientes/train"
