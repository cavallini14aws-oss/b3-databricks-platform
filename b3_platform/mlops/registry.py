from datetime import datetime, UTC

from pyspark.sql import Row
from pyspark.sql import types as T

from b3_platform.core.context import get_context


REGISTRY_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("algorithm", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("artifact_path", T.StringType(), True),
    ]
)


def register_model(
    spark,
    model_name: str,
    model_version: str,
    algorithm: str,
    run_id: str,
    status: str,
    artifact_path: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_model_registry")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(UTC).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        algorithm=algorithm,
        run_id=run_id,
        status=status,
        artifact_path=artifact_path,
    )

    df = spark.createDataFrame([row], schema=REGISTRY_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
