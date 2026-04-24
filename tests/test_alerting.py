import pytest
pytestmark = pytest.mark.heavy

import pytest

from pyspark.sql import Row

from data_platform.mlops.alerting import (
    ALERT_CONFIG_SCHEMA,
    ALERT_EVENT_SCHEMA,
    build_alert_events_from_drift_rows,
    build_alert_message,
    classify_alert_severity,
    determine_notification_status,
    dispatch_pending_alert_events,
    dispatch_planned_alert_events,
    load_alert_events_by_status,
    persist_dispatched_alert_events,
    plan_notifications_for_alert_events,
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
        "notification_error",
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


def test_determine_notification_status_returns_skipped_when_alerting_disabled():
    status = determine_notification_status(
        alerting_enabled=False,
        plan=[{"channel": "email", "recipients": ["a@test.com"]}],
    )

    assert status == "SKIPPED"


def test_determine_notification_status_returns_no_channel_when_plan_empty():
    status = determine_notification_status(
        alerting_enabled=True,
        plan=[],
    )

    assert status == "NO_CHANNEL"


def test_determine_notification_status_returns_no_recipients_when_email_has_no_recipients():
    status = determine_notification_status(
        alerting_enabled=True,
        plan=[{"channel": "email", "recipients": []}],
    )

    assert status == "NO_RECIPIENTS"


def test_determine_notification_status_returns_planned_when_plan_is_valid():
    status = determine_notification_status(
        alerting_enabled=True,
        plan=[{"channel": "slack", "message": "msg"}],
    )

    assert status == "PLANNED"


def test_plan_notifications_for_alert_events_marks_planned(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.alerting.load_alerting_config",
        lambda config_path: {
            "enable_alerting": True,
            "email_enabled": True,
            "slack_enabled": False,
            "teams_enabled": False,
            "recipients": "a@test.com",
        },
    )

    events = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "source_component": "drift_monitoring",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "severity": "CRITICAL",
            "message": "Drift critico",
            "notification_status": "PENDING",
        }
    ]

    planned = plan_notifications_for_alert_events(
        alert_events=events,
        config_path="config/env/dev.yml",
    )

    assert len(planned) == 1
    assert planned[0]["notification_status"] == "PLANNED"
    assert planned[0]["notification_plan"][0]["channel"] == "email"


def test_plan_notifications_for_alert_events_marks_skipped_when_alerting_disabled(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.alerting.load_alerting_config",
        lambda config_path: {
            "enable_alerting": False,
            "email_enabled": True,
            "recipients": "a@test.com",
        },
    )

    events = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "source_component": "drift_monitoring",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "severity": "CRITICAL",
            "message": "Drift critico",
            "notification_status": "PENDING",
        }
    ]

    planned = plan_notifications_for_alert_events(
        alert_events=events,
        config_path="config/env/dev.yml",
    )

    assert planned[0]["notification_status"] == "SKIPPED"


def test_plan_notifications_for_alert_events_marks_no_channel(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.alerting.load_alerting_config",
        lambda config_path: {
            "enable_alerting": True,
            "email_enabled": False,
            "slack_enabled": False,
            "teams_enabled": False,
            "recipients": "",
        },
    )

    events = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "source_component": "drift_monitoring",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "severity": "CRITICAL",
            "message": "Drift critico",
            "notification_status": "PENDING",
        }
    ]

    planned = plan_notifications_for_alert_events(
        alert_events=events,
        config_path="config/env/dev.yml",
    )

    assert planned[0]["notification_status"] == "NO_CHANNEL"


def test_dispatch_planned_alert_events_marks_sent(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.alerting.load_alerting_config",
        lambda config_path: {
            "enable_alerting": True,
            "email_enabled": True,
            "recipients": "a@test.com",
        },
    )
    monkeypatch.setattr(
        "data_platform.mlops.alerting.send_notification_plan",
        lambda **kwargs: [
            {
                "channel": "email",
                "delivery_status": "SENT",
            }
        ],
    )

    events = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "source_component": "drift_monitoring",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "severity": "CRITICAL",
            "message": "Drift critico",
            "notification_status": "PLANNED",
            "notification_plan": [
                {
                    "channel": "email",
                    "recipients": ["a@test.com"],
                    "subject": "Alerta",
                    "body": "Mensagem",
                }
            ],
        }
    ]

    dispatched = dispatch_planned_alert_events(
        pending_events=events,
        config_path="config/env/dev.yml",
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(dispatched) == 1
    assert dispatched[0]["notification_status"] == "SENT"
    assert dispatched[0]["notification_error"] is None


def test_dispatch_planned_alert_events_marks_failed(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.alerting.load_alerting_config",
        lambda config_path: {
            "enable_alerting": True,
            "email_enabled": True,
            "recipients": "a@test.com",
        },
    )
    monkeypatch.setattr(
        "data_platform.mlops.alerting.send_notification_plan",
        lambda **kwargs: [
            {
                "channel": "email",
                "delivery_status": "FAILED",
                "delivery_error": "SMTP error",
            }
        ],
    )

    events = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "source_component": "drift_monitoring",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "severity": "CRITICAL",
            "message": "Drift critico",
            "notification_status": "PLANNED",
            "notification_plan": [
                {
                    "channel": "email",
                    "recipients": ["a@test.com"],
                    "subject": "Alerta",
                    "body": "Mensagem",
                }
            ],
        }
    ]

    dispatched = dispatch_planned_alert_events(
        pending_events=events,
        config_path="config/env/dev.yml",
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(dispatched) == 1
    assert dispatched[0]["notification_status"] == "FAILED"
    assert "SMTP error" in dispatched[0]["notification_error"]


def test_dispatch_planned_alert_events_keeps_non_planned_status():
    events = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "source_component": "drift_monitoring",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "severity": "CRITICAL",
            "message": "Drift critico",
            "notification_status": "NO_CHANNEL",
            "notification_plan": [],
        }
    ]

    dispatched = dispatch_planned_alert_events(
        pending_events=events,
        config_path="config/env/dev.yml",
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(dispatched) == 1
    assert dispatched[0]["notification_status"] == "NO_CHANNEL"


def test_load_alert_events_by_status_returns_empty_when_table_missing():
    class FakeCatalog:
        @staticmethod
        def tableExists(name):
            return False

    class FakeSpark:
        catalog = FakeCatalog()

    result = load_alert_events_by_status(
        spark=FakeSpark(),
        notification_status="PLANNED",
        project="clientes",
        use_catalog=False,
    )

    assert result == []


def test_persist_dispatched_alert_events_builds_rows(monkeypatch):
    from types import SimpleNamespace

    captured_rows = []

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            schema_mlops="clientes_mlops",
            qualified_schema=lambda schema: schema,
            qualified_table=lambda schema, table: f"{schema}.{table}",
        ),
    )

    monkeypatch.setattr(
        "data_platform.mlops.alerting.get_context",
        lambda project, use_catalog: fake_ctx,
    )
    monkeypatch.setattr(
        "data_platform.mlops.alerting.persist_alert_events",
        lambda **kwargs: captured_rows.extend(kwargs["rows"]),
    )

    dispatched = [
        {
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "run_id": "run-1",
            "source_component": "drift_monitoring",
            "metric_name": "prediction_rate",
            "entity_name": "1.0",
            "baseline_value": 0.2,
            "current_value": 0.95,
            "severity": "CRITICAL",
            "message": "Drift critico",
            "notification_status": "SENT",
            "notification_error": None,
        }
    ]

    rows = persist_dispatched_alert_events(
        spark=object(),
        dispatched_events=dispatched,
        project="clientes",
        use_catalog=False,
    )

    assert len(rows) == 1
    assert len(captured_rows) == 1
    assert captured_rows[0]["notification_status"] == "SENT"


def test_dispatch_pending_alert_events_runs_full_flow(monkeypatch):
    from pyspark.sql import Row

    monkeypatch.setattr(
        "data_platform.mlops.alerting.load_alert_events_by_status",
        lambda **kwargs: [
            {
                "model_name": "clientes_status_classifier",
                "model_version": "v123",
                "run_id": "run-1",
                "source_component": "drift_monitoring",
                "metric_name": "prediction_rate",
                "entity_name": "1.0",
                "baseline_value": 0.2,
                "current_value": 0.95,
                "severity": "CRITICAL",
                "message": "Drift critico",
                "notification_status": "PLANNED",
                "notification_plan": [
                    {
                        "channel": "email",
                        "recipients": ["a@test.com"],
                        "subject": "Alerta",
                        "body": "Mensagem",
                    }
                ],
            }
        ],
    )
    monkeypatch.setattr(
        "data_platform.mlops.alerting.dispatch_planned_alert_events",
        lambda **kwargs: [
            {
                "model_name": "clientes_status_classifier",
                "model_version": "v123",
                "run_id": "run-1",
                "source_component": "drift_monitoring",
                "metric_name": "prediction_rate",
                "entity_name": "1.0",
                "baseline_value": 0.2,
                "current_value": 0.95,
                "severity": "CRITICAL",
                "message": "Drift critico",
                "notification_status": "SENT",
                "notification_error": None,
            }
        ],
    )
    monkeypatch.setattr(
        "data_platform.mlops.alerting.persist_dispatched_alert_events",
        lambda **kwargs: [
            Row(
                event_timestamp=None,
                env="dev",
                project="clientes",
                model_name="clientes_status_classifier",
                model_version="v123",
                run_id="run-1",
                source_component="drift_monitoring",
                metric_name="prediction_rate",
                entity_name="1.0",
                baseline_value=0.2,
                current_value=0.95,
                severity="CRITICAL",
                message="Drift critico",
                notification_status="SENT",
                notification_error=None,
            )
        ],
    )

    rows = dispatch_pending_alert_events(
        spark=object(),
        config_path="config/env/dev.yml",
        secrets_resolver=lambda scope, key: "x",
        project="clientes",
        use_catalog=False,
    )

    assert len(rows) == 1
    assert rows[0]["notification_status"] == "SENT"
