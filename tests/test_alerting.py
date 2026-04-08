from data_platform.mlops.alerting import (
    ALERT_CONFIG_SCHEMA,
    ALERT_EVENT_SCHEMA,
    build_alert_events_from_drift_rows,
    build_alert_message,
    classify_alert_severity,
    should_emit_alert,
)


def test_alert_config_schema_has_expected_fields():
    field_names = [field.name for field in ALERT_CONFIG_SCHEMA.fields]

    assert field_names == [
        "alert_name",
        "is_enabled",
        "model_name",
        "metric_name",
        "severity_min",
        "threshold_warning",
        "threshold_critical",
        "email_enabled",
        "slack_enabled",
        "teams_enabled",
        "owner_group",
        "recipients",
        "webhook_url_key",
        "notes",
    ]


def test_alert_event_schema_has_expected_fields():
    field_names = [field.name for field in ALERT_EVENT_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "run_id",
        "source_component",
        "metric_name",
        "entity_name",
        "baseline_value",
        "current_value",
        "severity",
        "message",
        "notification_status",
    ]


def test_classify_alert_severity_returns_expected_values():
    assert classify_alert_severity("OK") == "OK"
    assert classify_alert_severity("warning") == "WARNING"
    assert classify_alert_severity("CRITICAL") == "CRITICAL"
    assert classify_alert_severity("unknown") == "OK"


def test_should_emit_alert_respects_minimum_severity():
    assert should_emit_alert("WARNING", severity_min="WARNING") is True
    assert should_emit_alert("CRITICAL", severity_min="WARNING") is True
    assert should_emit_alert("OK", severity_min="WARNING") is False
    assert should_emit_alert("WARNING", severity_min="CRITICAL") is False


def test_build_alert_message_contains_expected_payload():
    message = build_alert_message(
        model_name="clientes_status_classifier",
        model_version="v123",
        metric_name="prediction_rate",
        entity_name="1.0",
        severity="CRITICAL",
        baseline_value=0.2,
        current_value=0.95,
    )

    assert "clientes_status_classifier" in message
    assert "v123" in message
    assert "prediction_rate" in message
    assert "1.0" in message
    assert "CRITICAL" in message


def test_build_alert_events_from_drift_rows_filters_ok_status():
    drift_rows = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "drift_status": "CRITICAL",
        },
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-2",
            "metric_name": "null_rate",
            "entity_name": "tem_file",
            "baseline_value": 0.0,
            "current_value": 0.0,
            "drift_status": "OK",
        },
    ]

    events = build_alert_events_from_drift_rows(
        drift_rows=drift_rows,
        severity_min="WARNING",
    )

    assert len(events) == 1
    assert events[0]["severity"] == "CRITICAL"
    assert events[0]["run_id"] == "run-1"
    assert events[0]["notification_status"] == "PENDING"


def test_build_alert_events_from_drift_rows_keeps_warning_and_critical():
    drift_rows = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-warning",
            "metric_name": "mean_value",
            "entity_name": "qtd_registros",
            "baseline_value": 1.0,
            "current_value": 1.2,
            "drift_status": "WARNING",
        },
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-critical",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "drift_status": "CRITICAL",
        },
    ]

    events = build_alert_events_from_drift_rows(
        drift_rows=drift_rows,
        severity_min="WARNING",
    )

    assert len(events) == 2
    assert events[0]["severity"] == "WARNING"
    assert events[1]["severity"] == "CRITICAL"
