from uuid import uuid4

from data_platform.governance.governance_runs import log_governance_run
from data_platform.mlops.deployments import (
    activate_model_deployment,
    get_active_model_deployment,
)
from data_platform.mlops.registry import update_model_status
from data_platform.mlops.model_states import ROLLED_BACK


def validate_rollback_target(target_env: str) -> None:
    valid_targets = {"hml", "prd"}
    if target_env not in valid_targets:
        raise ValueError(
            f"Rollback invalido para target_env={target_env}. "
            f"Permitidos: {sorted(valid_targets)}"
        )


def _get_previous_candidate(
    spark,
    model_name: str,
    target_env: str,
    current_model_version: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    from data_platform.core.context import get_context
    from pyspark.sql import functions as F

    ctx = get_context(project=project, use_catalog=use_catalog)
    table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_model_deployments")

    if not spark.catalog.tableExists(table_name):
        raise ValueError(
            f"Tabela de deployments nao encontrada para rollback: {table_name}"
        )

    rows = (
        spark.table(table_name)
        .filter(
            (F.col("model_name") == model_name) &
            (F.col("target_env") == target_env) &
            (F.col("model_version") != current_model_version)
        )
        .orderBy(F.col("event_timestamp").desc())
        .collect()
    )

    seen = set()
    for row in rows:
        version = row["model_version"]
        if version in seen:
            continue
        seen.add(version)
        if row["deployment_status"] != "INACTIVE":
            continue
        return row

    raise ValueError(
        f"Nenhuma versao anterior elegivel encontrada para rollback de "
        f"model_name={model_name}, target_env={target_env}"
    )


def prepare_rollback_request(
    spark,
    model_name: str,
    target_env: str,
    project: str = "clientes",
    use_catalog: bool = False,
    forced_run_id: str | None = None,
):
    validate_rollback_target(target_env)
    governance_run_id = forced_run_id or str(uuid4())

    log_governance_run(
        spark=spark,
        component="rollback",
        model_name=model_name,
        model_version=None,
        source_env=None,
        target_env=target_env,
        artifact_path=None,
        status="STARTED",
        run_id=governance_run_id,
        message="Rollback iniciado",
        project=project,
        use_catalog=use_catalog,
    )

    try:
        active = get_active_model_deployment(
            spark=spark,
            model_name=model_name,
            target_env=target_env,
            project=project,
            use_catalog=use_catalog,
        )

        if active is None:
            raise ValueError(
                f"Nenhum deployment ativo encontrado para model_name={model_name}, target_env={target_env}"
            )

        previous_candidate = _get_previous_candidate(
            spark=spark,
            model_name=model_name,
            target_env=target_env,
            current_model_version=active["model_version"],
            project=project,
            use_catalog=use_catalog,
        )

        activate_model_deployment(
            spark=spark,
            model_name=previous_candidate["model_name"],
            model_version=previous_candidate["model_version"],
            artifact_path=previous_candidate["artifact_path"],
            source_env=previous_candidate["source_env"],
            target_env=previous_candidate["target_env"],
            deployment_status="ROLLED_BACK_ACTIVE",
            run_id=previous_candidate["run_id"],
            notes=f"Rollback ativou versao {previous_candidate['model_version']}",
            project=project,
            use_catalog=use_catalog,
        )

        update_model_status(
            spark=spark,
            model_name=active["model_name"],
            model_version=active["model_version"],
            status=ROLLED_BACK,
            project=project,
            use_catalog=use_catalog,
        )

        log_governance_run(
            spark=spark,
            component="rollback",
            model_name=model_name,
            model_version=previous_candidate["model_version"],
            source_env=previous_candidate["source_env"],
            target_env=target_env,
            artifact_path=previous_candidate["artifact_path"],
            status="SUCCESS",
            run_id=governance_run_id,
            message=f"Rollback executado de {active['model_version']} para {previous_candidate['model_version']}",
            project=project,
            use_catalog=use_catalog,
        )

        return {
            "action": "ROLLBACK_EXECUTED",
            "model_name": model_name,
            "rolled_back_from_version": active["model_version"],
            "rolled_back_to_version": previous_candidate["model_version"],
            "artifact_path": previous_candidate["artifact_path"],
            "target_env": target_env,
            "status": ROLLED_BACK,
        }
    except Exception as exc:
        log_governance_run(
            spark=spark,
            component="rollback",
            model_name=model_name,
            model_version=None,
            source_env=None,
            target_env=target_env,
            artifact_path=None,
            status="ERROR",
            run_id=governance_run_id,
            message=f"{type(exc).__name__}: {str(exc)}",
            project=project,
            use_catalog=use_catalog,
        )
        raise
