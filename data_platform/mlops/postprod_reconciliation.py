from pyspark.sql import functions as F


def apply_time_window(
    df,
    *,
    timestamp_col: str,
    window_start: str | None = None,
    window_end: str | None = None,
):
    result = df

    if window_start:
        result = result.filter(F.col(timestamp_col) >= F.lit(window_start))

    if window_end:
        result = result.filter(F.col(timestamp_col) < F.lit(window_end))

    return result


def build_postprod_reconciliation_df(
    *,
    predictions_df,
    labels_df,
    join_keys: list[str],
    prediction_col: str = "prediction",
    label_col: str = "label",
    prediction_timestamp_col: str | None = None,
    label_timestamp_col: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
):
    if not join_keys:
        raise ValueError("join_keys deve conter ao menos uma chave")

    pred_df = predictions_df
    lab_df = labels_df

    if prediction_timestamp_col:
        pred_df = apply_time_window(
            pred_df,
            timestamp_col=prediction_timestamp_col,
            window_start=window_start,
            window_end=window_end,
        )

    if label_timestamp_col:
        lab_df = apply_time_window(
            lab_df,
            timestamp_col=label_timestamp_col,
            window_start=window_start,
            window_end=window_end,
        )

    pred_select = join_keys + [prediction_col]
    lab_select = join_keys + [label_col]

    reconciled_df = (
        pred_df.select(*pred_select)
        .join(lab_df.select(*lab_select), on=join_keys, how="inner")
        .select(
            *[F.col(key) for key in join_keys],
            F.col(prediction_col).cast("double").alias("prediction"),
            F.col(label_col).cast("double").alias("label"),
        )
    )

    return reconciled_df


def reconcile_postprod_from_tables(
    spark,
    *,
    predictions_table: str,
    labels_table: str,
    join_keys: list[str],
    prediction_col: str = "prediction",
    label_col: str = "label",
    prediction_timestamp_col: str | None = None,
    label_timestamp_col: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
):
    predictions_df = spark.table(predictions_table)
    labels_df = spark.table(labels_table)

    return build_postprod_reconciliation_df(
        predictions_df=predictions_df,
        labels_df=labels_df,
        join_keys=join_keys,
        prediction_col=prediction_col,
        label_col=label_col,
        prediction_timestamp_col=prediction_timestamp_col,
        label_timestamp_col=label_timestamp_col,
        window_start=window_start,
        window_end=window_end,
    )
