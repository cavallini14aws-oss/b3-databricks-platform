from datetime import datetime, timezone

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

PREDICTION_BASELINE_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("prediction_value", T.DoubleType(), True),
        T.StructField("prediction_count", T.LongType(), False),
        T.StructField("prediction_rate", T.DoubleType(), True),
        T.StructField("artifact_path", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
    ]
)

FEATURE_BASELINE_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("feature_name", T.StringType(), False),
        T.StructField("data_type", T.StringType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("null_count", T.LongType(), False),
        T.StructField("null_rate", T.DoubleType(), True),
        T.StructField("distinct_count", T.LongType(), False),
        T.StructField("mean_value", T.DoubleType(), True),
        T.StructField("min_value", T.StringType(), True),
        T.StructField("max_value", T.StringType(), True),
        T.StructField("artifact_path", T.StringType(), True),
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
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
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


def log_prediction_baseline(
    spark,
    predictions_df,
    model_name: str,
    model_version: str,
    artifact_path: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_prediction_baseline",
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    total_count = predictions_df.count()
    if total_count == 0:
        return

    grouped_rows = (
        predictions_df.groupBy("prediction")
        .count()
        .withColumnRenamed("count", "prediction_count")
        .collect()
    )

    rows = []
    event_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    for row in grouped_rows:
        prediction_count = int(row["prediction_count"])
        prediction_rate = float(prediction_count / total_count) if total_count else 0.0

        rows.append(
            Row(
                event_timestamp=event_timestamp,
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                prediction_value=float(row["prediction"]) if row["prediction"] is not None else None,
                prediction_count=prediction_count,
                prediction_rate=prediction_rate,
                artifact_path=artifact_path,
                run_id=run_id,
            )
        )

    if not rows:
        return

    df = spark.createDataFrame(rows, schema=PREDICTION_BASELINE_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def log_feature_baseline(
    spark,
    dataset_df,
    feature_columns: list[str],
    model_name: str,
    model_version: str,
    artifact_path: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_feature_baseline",
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row_count = dataset_df.count()
    if row_count == 0:
        return

    dtype_map = dict(dataset_df.dtypes)
    rows = []
    event_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    numeric_types = {
        "int",
        "bigint",
        "double",
        "float",
        "long",
        "short",
        "decimal",
        "smallint",
        "tinyint",
    }

    for feature_name in feature_columns:
        if feature_name not in dtype_map:
            continue

        data_type = dtype_map[feature_name]
        null_count = dataset_df.filter(F.col(feature_name).isNull()).count()
        distinct_count = dataset_df.select(feature_name).distinct().count()
        null_rate = float(null_count / row_count) if row_count else 0.0

        mean_value = None
        min_value = None
        max_value = None

        if any(token in data_type.lower() for token in numeric_types):
            metric_row = dataset_df.select(
                F.avg(F.col(feature_name)).alias("mean_value"),
                F.min(F.col(feature_name)).alias("min_value"),
                F.max(F.col(feature_name)).alias("max_value"),
            ).collect()[0]

            mean_value = (
                float(metric_row["mean_value"])
                if metric_row["mean_value"] is not None
                else None
            )
            min_value = (
                str(metric_row["min_value"])
                if metric_row["min_value"] is not None
                else None
            )
            max_value = (
                str(metric_row["max_value"])
                if metric_row["max_value"] is not None
                else None
            )
        else:
            metric_row = dataset_df.select(
                F.min(F.col(feature_name)).alias("min_value"),
                F.max(F.col(feature_name)).alias("max_value"),
            ).collect()[0]

            min_value = (
                str(metric_row["min_value"])
                if metric_row["min_value"] is not None
                else None
            )
            max_value = (
                str(metric_row["max_value"])
                if metric_row["max_value"] is not None
                else None
            )

        rows.append(
            Row(
                event_timestamp=event_timestamp,
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                feature_name=feature_name,
                data_type=data_type,
                row_count=int(row_count),
                null_count=int(null_count),
                null_rate=float(null_rate),
                distinct_count=int(distinct_count),
                mean_value=mean_value,
                min_value=min_value,
                max_value=max_value,
                artifact_path=artifact_path,
                run_id=run_id,
            )
        )

    if not rows:
        return

    df = spark.createDataFrame(rows, schema=FEATURE_BASELINE_SCHEMA)

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
