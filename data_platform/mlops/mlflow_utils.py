from __future__ import annotations

import mlflow


VALID_MLFLOW_STAGES = {"train", "evaluate"}


def build_mlflow_experiment_path(project: str, stage: str) -> str:
    normalized_stage = stage.strip().lower()

    if normalized_stage not in VALID_MLFLOW_STAGES:
        raise ValueError(
            f"MLflow stage invalido: {stage}. "
            f"Permitidos: {sorted(VALID_MLFLOW_STAGES)}"
        )

    return f"/Shared/mlops/{project}/{normalized_stage}"


def _ensure_workspace_parent_path(experiment_path: str) -> None:
    parent_path = experiment_path.rsplit("/", 1)[0]

    if not parent_path:
        return

    dbutils_created = False
    sdk_created = False
    errors = []

    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.getActiveSession()
        if spark is not None:
            dbutils = DBUtils(spark)
            dbutils.notebook.entry_point.getDbutils().workspace().mkdirs(parent_path)
            dbutils_created = True
    except Exception as exc:
        errors.append(f"dbutils workspace mkdirs falhou: {exc}")

    if not dbutils_created:
        try:
            from databricks.sdk import WorkspaceClient

            ws = WorkspaceClient()
            ws.workspace.mkdirs(parent_path)
            sdk_created = True
        except Exception as exc:
            errors.append(f"databricks sdk workspace mkdirs falhou: {exc}")

    if not dbutils_created and not sdk_created:
        raise RuntimeError(
            "Nao foi possivel criar o parent path do experimento MLflow no workspace. "
            f"parent_path={parent_path}. Detalhes: {' | '.join(errors)}"
        )


def set_mlflow_experiment_for_project(project: str, stage: str) -> str:
    experiment_path = build_mlflow_experiment_path(project=project, stage=stage)
    _ensure_workspace_parent_path(experiment_path)
    mlflow.set_experiment(experiment_path)
    return experiment_path
