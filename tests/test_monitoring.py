from data_platform.mlops.monitoring import (
    FEATURE_MONITORING_SCHEMA,
    PREDICTION_MONITORING_SCHEMA,
)


def test_prediction_monitoring_schema_has_expected_fields():
    field_names = [field.name for field in PREDICTION_MONITORING_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "target_env",
        "run_id",
        "prediction_value",
        "prediction_count",
        "prediction_rate",
    ]


def test_feature_monitoring_schema_has_expected_fields():
    field_names = [field.name for field in FEATURE_MONITORING_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "target_env",
        "run_id",
        "feature_name",
        "data_type",
        "row_count",
        "null_count",
        "null_rate",
        "distinct_count",
        "mean_value",
        "min_value",
        "max_value",
    ]
