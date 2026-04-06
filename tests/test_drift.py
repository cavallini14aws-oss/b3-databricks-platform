from data_platform.mlops.drift import (
    DRIFT_MONITORING_SCHEMA,
    classify_drift,
    compute_relative_diff,
)


def test_drift_monitoring_schema_has_expected_fields():
    field_names = [field.name for field in DRIFT_MONITORING_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "target_env",
        "run_id",
        "monitoring_type",
        "entity_name",
        "metric_name",
        "baseline_value",
        "current_value",
        "absolute_diff",
        "relative_diff",
        "drift_status",
    ]


def test_compute_relative_diff_with_non_zero_baseline():
    absolute_diff, relative_diff = compute_relative_diff(10.0, 12.0)

    assert absolute_diff == 2.0
    assert relative_diff == 0.2


def test_compute_relative_diff_with_zero_baseline():
    absolute_diff, relative_diff = compute_relative_diff(0.0, 0.25)

    assert absolute_diff == 0.25
    assert relative_diff == 0.25


def test_classify_drift_ok():
    assert classify_drift(0.05) == "OK"


def test_classify_drift_warning():
    assert classify_drift(0.15) == "WARNING"


def test_classify_drift_critical():
    assert classify_drift(0.35) == "CRITICAL"
