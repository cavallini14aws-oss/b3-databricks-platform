import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import Row
from pyspark.sql import types as T

from data_platform.core.context import get_context
from data_platform.governance.governance_runs import log_governance_run
from data_platform.mlops.deployments import (
    activate_model_deployment,
    get_active_model_deployment,
)
from data_platform.mlops.model_states import (
    DEPLOYED_HML,
    DEPLOYED_PRD,
    EVALUATED,
    PROMOTED_HML,
    PROMOTED_PRD,
    PROMOTION_APPROVED,
    ROLLED_BACK,
    TRAINED,
)
from data_platform.mlops.registry import (
    get_latest_valid_model_entry,
    update_model_status,
)


PROMOTABLE_SOURCE_STATES = {
    TRAINED,
    EVALUATED,
    PROMOTION_APPROVED,
    PROMOTED_HML,
    PROMOTED_PRD,
    DEPLOYED_HML,
    DEPLOYED_PRD,
    ROLLED_BACK,
}


@dataclass
class PromotionRequest:
    model_name: str
    source_env: str
    target_env: str
    model_version: str | None = None
    project: str = "clientes"
    use_catalog: bool = False


def validate_promotion_path(source_env: str, target_env: str) -> None:
    valid_paths = {
        ("dev", "hml"),
        ("hml", "prd"),
    }

    if (source_env, target_env) not in valid_paths:
        raise ValueError(
            f"Caminho de promocao invalido: {source_env} -> {target_env}. "
            "Permitidos: dev->hml, hml->prd"
        )


def resolve_model_entry(
    spark,
    request: PromotionRequest,
):
    if request.model_version:
        from data_platform.mlops.registry import get_model_registry_entry

        return get_model_registry_entry(
            spark=spark,
            model_name=request.model_name,
            model_version=request.model_version,
            project=request.project,
            use_catalog=request.use_catalog,
        )

    return get_latest_valid_model_entry(
        spark=spark,
        model_name=request.model_name,
        project=request.project,
        use_catalog=request.use_catalog,
    )


def promote_and_deploy_ml(
    spark,
    model_name: str,
    source_env: str,
    target_env: str,
    model_version: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
    forced_run_id: str | None = None,
):
    validate_promotion_path(source_env, target_env)

    governance_run_id = forced_run_id or str(uuid4())

    log_governance_run(
        spark=spark,
        component="promote_and_deploy_ml",
        model_name=model_name,
        model_version=model_version,
        source_env=source_env,
        target_env=target_env,
        artifact_path=None,
        status="STARTED",
        run_id=governance_run_id,
        message="Promotion iniciada",
        project=project,
        use_catalog=use_catalog,
    )

    request = PromotionRequest(
        model_name=model_name,
        source_env=source_env,
        target_env=target_env,
        model_version=model_version,
        project=project,
        use_catalog=use_catalog,
    )

    try:
        entry = resolve_model_entry(
            spark=spark,
            request=request,
        )

        resolved_model_version = entry["model_version"]
        artifact_path = entry["artifact_path"]
        deployment_run_id = entry["run_id"]
        status = entry["status"]

        if not artifact_path:
            raise ValueError(
                "Modelo sem artifact_path valido: "
                f"model_name={model_name}, model_version={resolved_model_version}"
            )

        if status not in PROMOTABLE_SOURCE_STATES:
            raise ValueError(
                "Status invalido para promocao: "
                f"model_name={model_name}, model_version={resolved_model_version}, status={status}. "
                f"Permitidos: {sorted(PROMOTABLE_SOURCE_STATES)}"
            )

        current_active = get_active_model_deployment(
            spark=spark,
            model_name=model_name,
            target_env=target_env,
            project=project,
            use_catalog=use_catalog,
        )

        if (
            current_active is not None
            and current_active["model_version"] == resolved_model_version
            and current_active["is_active"] is True
        ):
            log_governance_run(
                spark=spark,
                component="promote_and_deploy_ml",
                model_name=model_name,
                model_version=resolved_model_version,
                source_env=source_env,
                target_env=target_env,
                artifact_path=artifact_path,
                status="SUCCESS",
                run_id=governance_run_id,
                message="Model already active in target environment",
                project=project,
                use_catalog=use_catalog,
            )
            return {
                "model_name": model_name,
                "model_version": resolved_model_version,
                "artifact_path": artifact_path,
                "source_env": source_env,
                "target_env": target_env,
                "status": current_active["deployment_status"],
                "message": "model already active in target environment",
            }

        log_ml_promotion_event(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            artifact_path=artifact_path,
            source_env=source_env,
            target_env=target_env,
            status=PROMOTION_APPROVED,
            reason="Promotion request accepted",
            project=project,
            use_catalog=use_catalog,
        )

        update_model_status(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            status=PROMOTION_APPROVED,
            project=project,
            use_catalog=use_catalog,
        )

        promoted_status = PROMOTED_HML if target_env == "hml" else PROMOTED_PRD
        update_model_status(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            status=promoted_status,
            project=project,
            use_catalog=use_catalog,
        )

        deployed_status = DEPLOYED_HML if target_env == "hml" else DEPLOYED_PRD

        activate_model_deployment(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            artifact_path=artifact_path,
            source_env=source_env,
            target_env=target_env,
            deployment_status=deployed_status,
            run_id=deployment_run_id,
            notes=f"Promocao {source_env}->{target_env}",
            project=project,
            use_catalog=use_catalog,
        )

        update_model_status(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            status=deployed_status,
            project=project,
            use_catalog=use_catalog,
        )

        log_governance_run(
            spark=spark,
            component="promote_and_deploy_ml",
            model_name=model_name,
            model_version=resolved_model_version,
            source_env=source_env,
            target_env=target_env,
            artifact_path=artifact_path,
            status="SUCCESS",
            run_id=governance_run_id,
            message=f"Promotion concluida com status {deployed_status}",
            project=project,
            use_catalog=use_catalog,
        )

        return {
            "model_name": model_name,
            "model_version": resolved_model_version,
            "artifact_path": artifact_path,
            "source_env": source_env,
            "target_env": target_env,
            "status": deployed_status,
        }

    except Exception as exc:
        log_governance_run(
            spark=spark,
            component="promote_and_deploy_ml",
            model_name=model_name,
            model_version=model_version,
            source_env=source_env,
            target_env=target_env,
            artifact_path=None,
            status="ERROR",
            run_id=governance_run_id,
            message=f"{type(exc).__name__}: {str(exc)}",
            project=project,
            use_catalog=use_catalog,
        )
        raise


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Promote and deploy ML model")

    parser.add_argument("--model-name", required=True)
    parser.add_argument("--source-env", required=True, choices=["dev", "hml"])
    parser.add_argument("--target-env", required=True, choices=["hml", "prd"])
    parser.add_argument("--model-version", required=False, default=None)
    parser.add_argument("--project", required=False, default="clientes")
    parser.add_argument("--use-catalog", action="store_true")

    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        raise RuntimeError(
            "PySpark indisponivel para promote_and_deploy_ml. "
            "Execute este comando em ambiente com Spark/PySpark."
        ) from e

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    result = promote_and_deploy_ml(
        spark=spark,
        model_name=args.model_name,
        source_env=args.source_env,
        target_env=args.target_env,
        model_version=args.model_version,
        project=args.project,
        use_catalog=args.use_catalog,
    )

    print("Promotion result:")
    for k, v in result.items():
        print(f"{k}={v}")


if __name__ == "__main__":
    main()


def log_ml_promotion_event(
    spark,
    model_name: str,
    model_version: str,
    artifact_path: str,
    source_env: str,
    target_env: str,
    status: str,
    reason: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_ml_model_promotions",
    )

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    schema = T.StructType(
        [
            T.StructField("event_timestamp", T.TimestampType(), False),
            T.StructField("env", T.StringType(), False),
            T.StructField("project", T.StringType(), False),
            T.StructField("model_name", T.StringType(), False),
            T.StructField("model_version", T.StringType(), False),
            T.StructField("artifact_path", T.StringType(), False),
            T.StructField("source_env", T.StringType(), False),
            T.StructField("target_env", T.StringType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("reason", T.StringType(), False),
        ]
    )

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        artifact_path=artifact_path,
        source_env=source_env,
        target_env=target_env,
        status=status,
        reason=reason,
    )

    df = spark.createDataFrame([row], schema=schema)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
