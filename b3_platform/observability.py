from datetime import datetime

from pyspark.sql import Row

from b3_platform.context import get_context


def log_pipeline_event(
    spark,
    component: str,
    status: str,
    run_id: str,
    message: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    obs_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_obs}.log_pipeline_runs"
        if use_catalog
        else f"{ctx.naming.schema_obs}.log_pipeline_runs"
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_obs}")

    row = Row(
        event_timestamp=datetime.utcnow(),
        env=ctx.env,
        project=ctx.project,
        component=component,
        status=status,
        run_id=run_id,
        message=message,
    )

    df = spark.createDataFrame([row])

    if spark.catalog.tableExists(obs_table):
        df.write.mode("append").saveAsTable(obs_table)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(obs_table)