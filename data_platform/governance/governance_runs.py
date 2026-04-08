from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import types as T

from data_platform.core.context import get_context


GOVERNANCE_RUN_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("component", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("source_env", T.StringType(), True),
        T.StructField("target_env", T.StringType(), True),
        T.StructField("artifact_path", T.StringType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("message", T.StringType(), True),
    ]
)


def _get_governance_run_table_name(
    project: str = "clientes",
    use_catalog: bool = False,
) -> tuple[str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_ml_governance_runs",
    )
    return schema_name, table_name


def log_governance_run(
    spark,
    component: str,
    model_name: str,
    model_version: str | None,
    status: str,
    run_id: str,
    source_env: str | None = None,
    target_env: str | None = None,
    artifact_path: str | None = None,
    message: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name, table_name = _get_governance_run_table_name(
        project=project,
        use_catalog=use_catalog,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        component=component,
        model_name=model_name,
        model_version=model_version,
        source_env=source_env,
        target_env=target_env,
        artifact_path=artifact_path,
        status=status,
        run_id=run_id,
        message=message,
    )

    df = spark.createDataFrame([row], schema=GOVERNANCE_RUN_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
