from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform.core.context import get_context


PREDICTION_MONITORING_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("target_env", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("prediction_value", T.StringType(), False),
        T.StructField("prediction_count", T.LongType(), False),
        T.StructField("prediction_rate", T.DoubleType(), False),
    ]
)

FEATURE_MONITORING_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("target_env", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("feature_name", T.StringType(), False),
        T.StructField("data_type", T.StringType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("null_count", T.LongType(), False),
        T.StructField("null_rate", T.DoubleType(), False),
        T.StructField("distinct_count", T.LongType(), False),
        T.StructField("mean_value", T.DoubleType(), True),
        T.StructField("min_value", T.StringType(), True),
        T.StructField("max_value", T.StringType(), True),
    ]
)


def _get_monitoring_table_names(project: str = "clientes", use_catalog: bool = False) -> tuple[str, str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    prediction_table = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_prediction_monitoring",
    )
    feature_table = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_feature_monitoring",
    )
    return schema_name, prediction_table, feature_table


def log_prediction_monitoring(
    spark,
    predictions_df,
    model_name: str,
    model_version: str | None,
    target_env: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name, prediction_table, _ = _get_monitoring_table_names(
        project=project,
        use_catalog=use_catalog,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row_count = predictions_df.count()
    if row_count == 0:
        return

    grouped = (
        predictions_df.groupBy("prediction")
        .agg(F.count(F.lit(1)).alias("prediction_count"))
        .collect()
    )

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []

    for item in grouped:
        prediction_count = int(item["prediction_count"])
        prediction_value = str(item["prediction"])
        prediction_rate = float(prediction_count) / float(row_count)

        rows.append(
            Row(
                event_timestamp=now,
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                target_env=target_env,
                run_id=run_id,
                prediction_value=prediction_value,
                prediction_count=prediction_count,
                prediction_rate=prediction_rate,
            )
        )

    df = spark.createDataFrame(rows, schema=PREDICTION_MONITORING_SCHEMA)

    if spark.catalog.tableExists(prediction_table):
        df.write.mode("append").saveAsTable(prediction_table)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(prediction_table)


def log_feature_monitoring(
    spark,
    feature_df,
    feature_columns: list[str],
    model_name: str,
    model_version: str | None,
    target_env: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name, _, feature_table = _get_monitoring_table_names(
        project=project,
        use_catalog=use_catalog,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row_count = feature_df.count()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []

    numeric_types = {"int", "bigint", "float", "double", "decimal", "smallint", "tinyint", "long"}

    for field in feature_df.schema.fields:
        if field.name not in feature_columns:
            continue

        data_type = field.dataType.simpleString().lower()

        metrics_row = (
            feature_df.agg(
                F.count(F.lit(1)).alias("row_count"),
                F.sum(F.when(F.col(field.name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias("null_count"),
                F.countDistinct(F.col(field.name)).alias("distinct_count"),
            )
            .collect()[0]
        )

        null_count = int(metrics_row["null_count"] or 0)
        distinct_count = int(metrics_row["distinct_count"] or 0)
        null_rate = float(null_count) / float(row_count) if row_count else 0.0

        mean_value = None
        min_value = None
        max_value = None

        is_numeric = any(data_type.startswith(item) for item in numeric_types)

        if is_numeric:
            numeric_row = (
                feature_df.agg(
                    F.avg(F.col(field.name)).alias("mean_value"),
                    F.min(F.col(field.name)).alias("min_value"),
                    F.max(F.col(field.name)).alias("max_value"),
                )
                .collect()[0]
            )

            mean_raw = numeric_row["mean_value"]
            min_raw = numeric_row["min_value"]
            max_raw = numeric_row["max_value"]

            mean_value = float(mean_raw) if mean_raw is not None else None
            min_value = str(min_raw) if min_raw is not None else None
            max_value = str(max_raw) if max_raw is not None else None

        rows.append(
            Row(
                event_timestamp=now,
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                target_env=target_env,
                run_id=run_id,
                feature_name=field.name,
                data_type=data_type,
                row_count=int(row_count),
                null_count=null_count,
                null_rate=null_rate,
                distinct_count=distinct_count,
                mean_value=mean_value,
                min_value=min_value,
                max_value=max_value,
            )
        )

    if not rows:
        return

    df = spark.createDataFrame(rows, schema=FEATURE_MONITORING_SCHEMA)

    if spark.catalog.tableExists(feature_table):
        df.write.mode("append").saveAsTable(feature_table)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(feature_table)
