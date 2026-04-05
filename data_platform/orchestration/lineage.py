from datetime import datetime

from pyspark.sql import Row

from data_platform.core.context import get_context


def _ensure_schema(spark, qualified_schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")


def log_pipeline_lineage(
    spark,
    parent_component: str,
    parent_run_id: str,
    child_component: str,
    child_run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    obs_schema = ctx.naming.qualified_schema(ctx.naming.schema_obs)
    lineage_table = ctx.naming.obs_lineage_table

    _ensure_schema(spark, obs_schema)

    row = Row(
        event_timestamp=datetime.utcnow(),
        env=ctx.env,
        project=ctx.project,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
        child_component=child_component,
        child_run_id=child_run_id,
    )

    df = spark.createDataFrame([row])

    if spark.catalog.tableExists(lineage_table):
        df.write.mode("append").saveAsTable(lineage_table)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            lineage_table
        )
