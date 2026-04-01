from datetime import datetime

from pyspark.sql import Row

from b3_platform.context import get_context


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

    lineage_table = (
        f"{ctx.naming.catalog}.{ctx.naming.schema_obs}.log_pipeline_lineage"
        if use_catalog
        else f"{ctx.naming.schema_obs}.log_pipeline_lineage"
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_obs}")

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
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(lineage_table)