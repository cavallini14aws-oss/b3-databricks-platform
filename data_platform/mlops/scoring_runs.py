from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import types as T

from data_platform.core.context import get_context


SCORING_RUN_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("target_env", T.StringType(), False),
        T.StructField("input_table", T.StringType(), False),
        T.StructField("output_table", T.StringType(), False),
        T.StructField("input_count", T.LongType(), False),
        T.StructField("prediction_count", T.LongType(), False),
        T.StructField("artifact_path", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
    ]
)


def log_scoring_run(
    spark,
    model_name: str,
    model_version: str | None,
    target_env: str,
    input_table: str,
    output_table: str,
    input_count: int,
    prediction_count: int,
    artifact_path: str | None,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_scoring_runs",
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        target_env=target_env,
        input_table=input_table,
        output_table=output_table,
        input_count=int(input_count),
        prediction_count=int(prediction_count),
        artifact_path=artifact_path,
        run_id=run_id,
    )

    df = spark.createDataFrame([row], schema=SCORING_RUN_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
