from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform.core.context import get_context


DRIFT_MONITORING_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("target_env", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("monitoring_type", T.StringType(), False),
        T.StructField("entity_name", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("baseline_value", T.DoubleType(), True),
        T.StructField("current_value", T.DoubleType(), True),
        T.StructField("absolute_diff", T.DoubleType(), True),
        T.StructField("relative_diff", T.DoubleType(), True),
        T.StructField("drift_status", T.StringType(), False),
    ]
)


def compute_relative_diff(baseline_value: float | None, current_value: float | None) -> tuple[float | None, float | None]:
    if baseline_value is None or current_value is None:
        return None, None

    absolute_diff = abs(float(current_value) - float(baseline_value))

    if float(baseline_value) == 0.0:
        relative_diff = absolute_diff
    else:
        relative_diff = absolute_diff / abs(float(baseline_value))

    return absolute_diff, relative_diff


def classify_drift(relative_diff: float | None) -> str:
    if relative_diff is None:
        return "OK"
    if relative_diff < 0.10:
        return "OK"
    if relative_diff < 0.30:
        return "WARNING"
    return "CRITICAL"


def _get_drift_table_name(project: str = "clientes", use_catalog: bool = False) -> tuple[str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_drift_monitoring",
    )
    return schema_name, table_name


def log_drift_records(
    spark,
    rows: list[Row],
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    if not rows:
        return

    schema_name, table_name = _get_drift_table_name(project=project, use_catalog=use_catalog)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    df = spark.createDataFrame(rows, schema=DRIFT_MONITORING_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def compute_and_log_feature_drift(
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
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    row_count = feature_df.count()

    if row_count == 0:
        return

    rows = []
    numeric_types = {"int", "bigint", "float", "double", "decimal", "smallint", "tinyint", "long"}

    for field in feature_df.schema.fields:
        if field.name not in feature_columns:
            continue

        data_type = field.dataType.simpleString().lower()

        agg_row = (
            feature_df.agg(
                F.sum(F.when(F.col(field.name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias("null_count"),
                F.countDistinct(F.col(field.name)).alias("distinct_count"),
            )
            .collect()[0]
        )

        null_count = int(agg_row["null_count"] or 0)
        distinct_count = int(agg_row["distinct_count"] or 0)
        null_rate = float(null_count) / float(row_count) if row_count else 0.0

        baseline_null_rate = 0.0
        baseline_distinct_count = float(distinct_count)

        abs_diff, rel_diff = compute_relative_diff(baseline_null_rate, null_rate)
        rows.append(
            Row(
                event_timestamp=now,
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                target_env=target_env,
                run_id=run_id,
                monitoring_type="feature",
                entity_name=field.name,
                metric_name="null_rate",
                baseline_value=float(baseline_null_rate),
                current_value=float(null_rate),
                absolute_diff=abs_diff,
                relative_diff=rel_diff,
                drift_status=classify_drift(rel_diff),
            )
        )

        abs_diff, rel_diff = compute_relative_diff(baseline_distinct_count, float(distinct_count))
        rows.append(
            Row(
                event_timestamp=now,
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                target_env=target_env,
                run_id=run_id,
                monitoring_type="feature",
                entity_name=field.name,
                metric_name="distinct_count",
                baseline_value=float(baseline_distinct_count),
                current_value=float(distinct_count),
                absolute_diff=abs_diff,
                relative_diff=rel_diff,
                drift_status=classify_drift(rel_diff),
            )
        )

        if data_type.startswith(("int", "bigint", "float", "double", "decimal", "smallint", "tinyint", "long")):
            avg_row = feature_df.agg(F.avg(F.col(field.name)).alias("mean_value")).collect()[0]
            mean_value = avg_row["mean_value"]

            if mean_value is not None:
                baseline_mean = float(mean_value)
                abs_diff, rel_diff = compute_relative_diff(baseline_mean, float(mean_value))

                rows.append(
                    Row(
                        event_timestamp=now,
                        env=ctx.env,
                        project=ctx.project,
                        model_name=model_name,
                        model_version=model_version,
                        target_env=target_env,
                        run_id=run_id,
                        monitoring_type="feature",
                        entity_name=field.name,
                        metric_name="mean_value",
                        baseline_value=float(baseline_mean),
                        current_value=float(mean_value),
                        absolute_diff=abs_diff,
                        relative_diff=rel_diff,
                        drift_status=classify_drift(rel_diff),
                    )
                )

    log_drift_records(
        spark=spark,
        rows=rows,
        project=project,
        use_catalog=use_catalog,
    )


def compute_and_log_prediction_drift(
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
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    row_count = predictions_df.count()
    if row_count == 0:
        return

    grouped = (
        predictions_df.groupBy("prediction")
        .agg(F.count(F.lit(1)).alias("prediction_count"))
        .collect()
    )

    rows = []

    for item in grouped:
        prediction_count = int(item["prediction_count"])
        prediction_value = str(item["prediction"])
        prediction_rate = float(prediction_count) / float(row_count)

        baseline_rate = float(prediction_rate)
        abs_diff, rel_diff = compute_relative_diff(baseline_rate, prediction_rate)

        rows.append(
            Row(
                event_timestamp=now,
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                target_env=target_env,
                run_id=run_id,
                monitoring_type="prediction",
                entity_name=prediction_value,
                metric_name="prediction_rate",
                baseline_value=float(baseline_rate),
                current_value=float(prediction_rate),
                absolute_diff=abs_diff,
                relative_diff=rel_diff,
                drift_status=classify_drift(rel_diff),
            )
        )

    log_drift_records(
        spark=spark,
        rows=rows,
        project=project,
        use_catalog=use_catalog,
    )
