from data_platform.mlops.baseline import (
    FEATURE_BASELINE_SCHEMA,
    PREDICTION_BASELINE_SCHEMA,
)


def test_prediction_baseline_schema_has_expected_fields():
    field_names = {field.name for field in PREDICTION_BASELINE_SCHEMA.fields}

    expected = {
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "prediction_value",
        "prediction_count",
        "prediction_rate",
        "artifact_path",
        "run_id",
    }

    assert expected.issubset(field_names)


def test_feature_baseline_schema_has_expected_fields():
    field_names = {field.name for field in FEATURE_BASELINE_SCHEMA.fields}

    expected = {
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "feature_name",
        "data_type",
        "row_count",
        "null_count",
        "null_rate",
        "distinct_count",
        "mean_value",
        "min_value",
        "max_value",
        "artifact_path",
        "run_id",
    }

    assert expected.issubset(field_names)
