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

    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
    except Exception:
        return

    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            return

        dbutils = DBUtils(spark)
        dbutils.fs.mkdirs("dbfs:/tmp")  # no-op leve para garantir objeto funcional
        dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        dbutils.notebook.entry_point.getDbutils().workspace().mkdirs(parent_path)
    except Exception:
        # Fora de notebook Databricks ou sem permissao de workspace.
        # Nao quebrar o fluxo local por isso.
        return


def set_mlflow_experiment_for_project(project: str, stage: str) -> str:
    experiment_path = build_mlflow_experiment_path(project=project, stage=stage)
    _ensure_workspace_parent_path(experiment_path)
    mlflow.set_experiment(experiment_path)
    return experiment_path
