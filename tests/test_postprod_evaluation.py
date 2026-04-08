from data_platform.mlops.postprod_evaluation import POSTPROD_METRICS_SCHEMA


def test_postprod_metrics_schema_has_expected_fields():
    field_names = [field.name for field in POSTPROD_METRICS_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "metric_name",
        "metric_value",
        "window_start",
        "window_end",
        "run_id",
    ]


def test_evaluate_postprod_from_tables_calls_reconciliation_and_evaluation(monkeypatch):
    from data_platform.mlops.postprod_evaluation import evaluate_postprod_from_tables

    captured = {}

    monkeypatch.setattr(
        "data_platform.mlops.postprod_evaluation.reconcile_postprod_from_tables",
        lambda **kwargs: "reconciled-df",
    )

    def fake_evaluate(**kwargs):
        captured["predictions_df"] = kwargs["predictions_df"]
        captured["model_name"] = kwargs["model_name"]
        captured["model_version"] = kwargs["model_version"]
        captured["run_id"] = kwargs["run_id"]
        return {"accuracy": 0.9, "support": 100.0}

    monkeypatch.setattr(
        "data_platform.mlops.postprod_evaluation.evaluate_postprod_predictions",
        fake_evaluate,
    )

    result = evaluate_postprod_from_tables(
        spark=object(),
        predictions_table="clientes_mlops.tb_model_predictions",
        labels_table="clientes_gold.tb_labels_reais",
        join_keys=["id_cliente"],
        model_name="clientes_status_classifier",
        model_version="v123",
        run_id="run-1",
        prediction_col="prediction",
        label_col="label_real",
        prediction_timestamp_col="prediction_ts",
        label_timestamp_col="label_ts",
        window_start="2026-04-01",
        window_end="2026-04-08",
        metric_names=["accuracy"],
        project="clientes",
        use_catalog=False,
    )

    assert captured["predictions_df"] == "reconciled-df"
    assert captured["model_name"] == "clientes_status_classifier"
    assert captured["model_version"] == "v123"
    assert captured["run_id"] == "run-1"
    assert result["accuracy"] == 0.9
    assert result["support"] == 100.0
