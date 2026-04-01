from datetime import datetime

from pyspark.sql import Row

from b3_platform.core.context import get_context


def _ensure_schema(spark, qualified_schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")


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
        duration_seconds=duration_seconds,
    )

    df = spark.createDataFrame([row])

    if spark.catalog.tableExists(obs_table):
        df.write.mode("append").saveAsTable(obs_table)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            obs_table
        )
