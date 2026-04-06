from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform.core.context import get_context


DEPLOYMENT_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("artifact_path", T.StringType(), False),
        T.StructField("source_env", T.StringType(), True),
        T.StructField("target_env", T.StringType(), False),
        T.StructField("deployment_status", T.StringType(), False),
        T.StructField("is_active", T.BooleanType(), False),
        T.StructField("run_id", T.StringType(), True),
        T.StructField("notes", T.StringType(), True),
    ]
)


def _get_deployments_table_name(project: str = "clientes", use_catalog: bool = False) -> tuple[str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_model_deployments")
    return schema_name, table_name


def register_model_deployment(
    spark,
    model_name: str,
    model_version: str,
    artifact_path: str,
    source_env: str | None,
    target_env: str,
    deployment_status: str,
    is_active: bool,
    run_id: str | None = None,
    notes: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name, table_name = _get_deployments_table_name(project=project, use_catalog=use_catalog)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        artifact_path=artifact_path,
        source_env=source_env,
        target_env=target_env,
        deployment_status=deployment_status,
        is_active=is_active,
        run_id=run_id,
        notes=notes,
    )

    df = spark.createDataFrame([row], schema=DEPLOYMENT_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def get_active_model_deployment(
    spark,
    model_name: str,
    target_env: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    _, table_name = _get_deployments_table_name(project=project, use_catalog=use_catalog)

    if not spark.catalog.tableExists(table_name):
        return None

    rows = (
        spark.table(table_name)
        .filter(
            (F.col("model_name") == model_name) &
            (F.col("target_env") == target_env) &
            (F.col("is_active") == F.lit(True))
        )
        .orderBy(F.col("event_timestamp").desc())
        .limit(1)
        .collect()
    )

    if not rows:
        return None

    return rows[0]



def get_active_model_for_env(
    spark,
    model_name: str,
    target_env: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    active = get_active_model_deployment(
        spark=spark,
        model_name=model_name,
        target_env=target_env,
        project=project,
        use_catalog=use_catalog,
    )

    if active is None:
        return None

    return {
        "model_name": active["model_name"],
        "model_version": active["model_version"],
        "artifact_path": active["artifact_path"],
        "source_env": active["source_env"],
        "target_env": active["target_env"],
        "deployment_status": active["deployment_status"],
        "run_id": active["run_id"],
        "event_timestamp": active["event_timestamp"],
    }


def deactivate_active_model_deployment(
    spark,
    model_name: str,
    target_env: str,
    reason: str,
    project: str = "clientes",
    use_catalog: bool = False,
):
    active = get_active_model_deployment(
        spark=spark,
        model_name=model_name,
        target_env=target_env,
        project=project,
        use_catalog=use_catalog,
    )

    if active is None:
        return None

    register_model_deployment(
        spark=spark,
        model_name=active["model_name"],
        model_version=active["model_version"],
        artifact_path=active["artifact_path"],
        source_env=active["source_env"],
        target_env=active["target_env"],
        deployment_status="INACTIVE",
        is_active=False,
        run_id=active["run_id"],
        notes=reason,
        project=project,
        use_catalog=use_catalog,
    )

    return active


def activate_model_deployment(
    spark,
    model_name: str,
    model_version: str,
    artifact_path: str,
    source_env: str | None,
    target_env: str,
    deployment_status: str,
    run_id: str | None = None,
    notes: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
):
    previous_active = deactivate_active_model_deployment(
        spark=spark,
        model_name=model_name,
        target_env=target_env,
        reason=f"Desativado por nova ativacao da versao {model_version}",
        project=project,
        use_catalog=use_catalog,
    )

    register_model_deployment(
        spark=spark,
        model_name=model_name,
        model_version=model_version,
        artifact_path=artifact_path,
        source_env=source_env,
        target_env=target_env,
        deployment_status=deployment_status,
        is_active=True,
        run_id=run_id,
        notes=notes,
        project=project,
        use_catalog=use_catalog,
    )

    return previous_active
