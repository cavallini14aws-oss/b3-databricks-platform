from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import types as T

from b3_platform.core.context import get_context


def _ensure_schema(spark, qualified_schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")


OBSERVABILITY_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("component", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("message", T.StringType(), False),
        T.StructField("duration_seconds", T.DoubleType(), True),
    ]
)


def log_pipeline_event(
    spark,
    component: str,
    status: str,
    run_id: str,
    message: str,
    project: str = "clientes",
    use_catalog: bool = False,
    duration_seconds: float | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    obs_schema = ctx.naming.qualified_schema(ctx.naming.schema_obs)
    obs_table = ctx.naming.obs_runs_table

    _ensure_schema(spark, obs_schema)

    row = Row(
        event_timestamp=datetime.utcnow(),
        env=ctx.env,
        project=ctx.project,
        component=component,
        status=status,
        run_id=run_id,
        message=message,
        duration_seconds=float(duration_seconds) if duration_seconds is not None else None,
    )

    df = spark.createDataFrame([row], schema=OBSERVABILITY_SCHEMA)

    if spark.catalog.tableExists(obs_table):
        df.write.mode("append").saveAsTable(obs_table)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(obs_table)
