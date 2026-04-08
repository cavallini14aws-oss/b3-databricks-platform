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
