from datetime import datetime, UTC

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform.core.context import get_context


BASELINE_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("baseline_name", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("metric_value", T.DoubleType(), True),
        T.StructField("run_id", T.StringType(), False),
    ]
)


def log_baseline_metric(
    spark,
    model_name: str,
    model_version: str,
    baseline_name: str,
    metric_name: str,
    metric_value: float,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_model_baseline")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(UTC).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        baseline_name=baseline_name,
        metric_name=metric_name,
        metric_value=float(metric_value),
        run_id=run_id,
    )

    df = spark.createDataFrame([row], schema=BASELINE_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def compute_majority_baseline_accuracy(test_df) -> float:
    train_majority = 1.0
    baseline_predictions = test_df.withColumn("baseline_prediction", F.lit(train_majority))

    correct = baseline_predictions.filter(F.col("label") == F.col("baseline_prediction")).count()
    total = baseline_predictions.count()

    if total == 0:
        return 0.0

    return float(correct / total)
