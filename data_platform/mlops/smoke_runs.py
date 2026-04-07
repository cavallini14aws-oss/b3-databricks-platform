from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import types as T

from data_platform.core.context import get_context


SMOKE_RUN_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("component", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("input_table", T.StringType(), True),
        T.StructField("output_table", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("message", T.StringType(), True),
    ]
)


def _get_smoke_run_table_name(
    project: str = "clientes",
    use_catalog: bool = False,
) -> tuple[str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_ml_smoke_runs",
    )
    return schema_name, table_name


def log_smoke_run(
    spark,
    component: str,
    model_name: str,
    model_version: str | None,
    status: str,
    run_id: str,
    input_table: str | None = None,
    output_table: str | None = None,
    message: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name, table_name = _get_smoke_run_table_name(
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
        status=status,
        input_table=input_table,
        output_table=output_table,
        run_id=run_id,
        message=message,
    )

    df = spark.createDataFrame([row], schema=SMOKE_RUN_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
