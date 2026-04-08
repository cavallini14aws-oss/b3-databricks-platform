from datetime import datetime, timezone

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform.core.context import get_context
from data_platform.mlops.postprod_reconciliation import reconcile_postprod_from_tables


POSTPROD_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("metric_value", T.DoubleType(), True),
        T.StructField("window_start", T.StringType(), True),
        T.StructField("window_end", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
    ]
)


def _get_postprod_metrics_table_name(
    project: str = "clientes",
    use_catalog: bool = False,
) -> tuple[str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_postprod_metrics",
    )
    return schema_name, table_name


def compute_postprod_metrics(
    predictions_df,
    metric_names: list[str] | None = None,
) -> dict[str, float]:
    metric_names = metric_names or ["accuracy", "f1", "precision", "recall", "auc"]

    results = {}

    if "accuracy" in metric_names:
        results["accuracy"] = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy",
        ).evaluate(predictions_df)

    if "f1" in metric_names:
        results["f1"] = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1",
        ).evaluate(predictions_df)

    if "precision" in metric_names:
        results["precision"] = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedPrecision",
        ).evaluate(predictions_df)

    if "recall" in metric_names:
        results["recall"] = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedRecall",
        ).evaluate(predictions_df)

    if "auc" in metric_names:
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        results["auc"] = evaluator.evaluate(predictions_df)

    return {key: float(value) for key, value in results.items()}


def persist_postprod_metrics(
    spark,
    *,
    model_name: str,
    model_version: str | None,
    metrics: dict[str, float],
    window_start: str | None,
    window_end: str | None,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    if not metrics:
        return

    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name, table_name = _get_postprod_metrics_table_name(
        project=project,
        use_catalog=use_catalog,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    timestamp_value = datetime.now(timezone.utc).replace(tzinfo=None)

    rows = [
        Row(
            event_timestamp=timestamp_value,
            env=ctx.env,
            project=ctx.project,
            model_name=model_name,
            model_version=model_version,
            metric_name=metric_name,
            metric_value=float(metric_value),
            window_start=window_start,
            window_end=window_end,
            run_id=run_id,
        )
        for metric_name, metric_value in metrics.items()
    ]

    df = spark.createDataFrame(rows, schema=POSTPROD_METRICS_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def evaluate_postprod_predictions(
    spark,
    *,
    predictions_df,
    model_name: str,
    model_version: str | None,
    run_id: str,
    window_start: str | None = None,
    window_end: str | None = None,
    metric_names: list[str] | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
) -> dict[str, float]:
    support = predictions_df.count()
    if support == 0:
        raise ValueError("Nenhum registro encontrado para post-production evaluation")

    metrics = compute_postprod_metrics(
        predictions_df=predictions_df,
        metric_names=metric_names,
    )
    metrics["support"] = float(support)

    persist_postprod_metrics(
        spark=spark,
        model_name=model_name,
        model_version=model_version,
        metrics=metrics,
        window_start=window_start,
        window_end=window_end,
        run_id=run_id,
        project=project,
        use_catalog=use_catalog,
    )

    return metrics


def evaluate_postprod_from_tables(
    spark,
    *,
    predictions_table: str,
    labels_table: str,
    join_keys: list[str],
    model_name: str,
    model_version: str | None,
    run_id: str,
    prediction_col: str = "prediction",
    label_col: str = "label",
    prediction_timestamp_col: str | None = None,
    label_timestamp_col: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    metric_names: list[str] | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
) -> dict[str, float]:
    reconciled_df = reconcile_postprod_from_tables(
        spark=spark,
        predictions_table=predictions_table,
        labels_table=labels_table,
        join_keys=join_keys,
        prediction_col=prediction_col,
        label_col=label_col,
        prediction_timestamp_col=prediction_timestamp_col,
        label_timestamp_col=label_timestamp_col,
        window_start=window_start,
        window_end=window_end,
    )

    return evaluate_postprod_predictions(
        spark=spark,
        predictions_df=reconciled_df,
        model_name=model_name,
        model_version=model_version,
        run_id=run_id,
        window_start=window_start,
        window_end=window_end,
        metric_names=metric_names,
        project=project,
        use_catalog=use_catalog,
    )
