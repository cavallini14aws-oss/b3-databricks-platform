import pytest
pytestmark = pytest.mark.heavy

import pytest

import pytest

from data_platform.mlops.mlflow_utils import (
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


def test_set_mlflow_experiment_for_project_uses_expected_path(monkeypatch):
    captured = {"path": None}

    monkeypatch.setattr(
        "data_platform.mlops.mlflow_utils._ensure_workspace_parent_path",
        lambda experiment_path: None,
    )
    monkeypatch.setattr(
        "data_platform.mlops.mlflow_utils.mlflow.set_experiment",
        lambda experiment_path: captured.__setitem__("path", experiment_path),
    )

    result = set_mlflow_experiment_for_project(project="clientes", stage="train")

    assert result == "/Shared/mlops/clientes/train"
    assert captured["path"] == "/Shared/mlops/clientes/train"
