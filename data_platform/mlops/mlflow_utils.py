from __future__ import annotations


VALID_MLFLOW_STAGES = {"train", "evaluate"}


def _get_mlflow_module():
    try:
        import mlflow
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "mlflow nao esta instalado no ambiente atual. "
            "Instale a dependencia de runtime ML antes de executar esta etapa."
        ) from exc
    return mlflow


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
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient()
        ws.workspace.mkdirs(parent_path)
    except Exception as exc:
        raise RuntimeError(
            "Nao foi possivel criar o parent path do experimento MLflow no workspace "
            f"via Databricks SDK. parent_path={parent_path}. Erro: {exc}"
        ) from exc


def set_mlflow_experiment_for_project(project: str, stage: str) -> str:
    experiment_path = build_mlflow_experiment_path(project=project, stage=stage)
    _ensure_workspace_parent_path(experiment_path)
    mlflow = _get_mlflow_module()
    mlflow.set_experiment(experiment_path)
    return experiment_path
