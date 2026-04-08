from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import types as T

from data_platform.core.context import get_context
from data_platform.mlops.notifications import (
    build_notification_plan,
    load_alerting_config,
    send_notification_plan,
)


ALERT_CONFIG_SCHEMA = T.StructType(
    [
        T.StructField("alert_name", T.StringType(), False),
        T.StructField("is_enabled", T.BooleanType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("severity_min", T.StringType(), False),
        T.StructField("threshold_warning", T.DoubleType(), True),
        T.StructField("threshold_critical", T.DoubleType(), True),
        T.StructField("email_enabled", T.BooleanType(), False),
        T.StructField("slack_enabled", T.BooleanType(), False),
        T.StructField("teams_enabled", T.BooleanType(), False),
        T.StructField("owner_group", T.StringType(), True),
        T.StructField("recipients", T.StringType(), True),
        T.StructField("webhook_url_key", T.StringType(), True),
        T.StructField("notes", T.StringType(), True),
    ]
)


ALERT_EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("source_component", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("entity_name", T.StringType(), True),
        T.StructField("baseline_value", T.DoubleType(), True),
        T.StructField("current_value", T.DoubleType(), True),
        T.StructField("severity", T.StringType(), False),
        T.StructField("message", T.StringType(), True),
        T.StructField("notification_status", T.StringType(), False),
    ]
)


SEVERITY_ORDER = {
    "OK": 0,
    "WARNING": 1,
    "CRITICAL": 2,
}


def _get_alert_event_table_name(
    project: str = "clientes",
    use_catalog: bool = False,
) -> tuple[str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_ml_alert_events",
    )
    return schema_name, table_name


def classify_alert_severity(drift_status: str) -> str:
    normalized = (drift_status or "").strip().upper()

    if normalized in {"CRITICAL", "WARNING", "OK"}:
        return normalized

    return "OK"


def should_emit_alert(
    drift_status: str,
    severity_min: str = "WARNING",
) -> bool:
    current = SEVERITY_ORDER.get(classify_alert_severity(drift_status), 0)
    minimum = SEVERITY_ORDER.get((severity_min or "WARNING").strip().upper(), 1)
    return current >= minimum


def build_alert_message(
    model_name: str,
    model_version: str | None,
    metric_name: str,
    entity_name: str | None,
    severity: str,
    baseline_value: float | None,
    current_value: float | None,
) -> str:
    entity_part = f", entity_name={entity_name}" if entity_name else ""
    return (
        f"Alerta de modelo: model_name={model_name}, "
        f"model_version={model_version}, "
        f"metric_name={metric_name}{entity_part}, "
        f"severity={severity}, "
        f"baseline_value={baseline_value}, "
        f"current_value={current_value}"
    )


def build_alert_event_row(
    *,
    env: str,
    project: str,
    model_name: str,
    model_version: str | None,
    run_id: str,
    source_component: str,
    metric_name: str,
    entity_name: str | None,
    baseline_value: float | None,
    current_value: float | None,
    severity: str,
    message: str,
    notification_status: str = "PENDING",
):
    return Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=env,
        project=project,
        model_name=model_name,
        model_version=model_version,
        run_id=run_id,
        source_component=source_component,
        metric_name=metric_name,
        entity_name=entity_name,
        baseline_value=baseline_value,
        current_value=current_value,
        severity=severity,
        message=message,
        notification_status=notification_status,
    )


def build_alert_events_from_drift_rows(
    drift_rows: list[dict],
    severity_min: str = "WARNING",
) -> list[dict]:
    events = []

    for row in drift_rows:
        severity = classify_alert_severity(row["drift_status"])
        if not should_emit_alert(severity, severity_min=severity_min):
            continue

        message = build_alert_message(
            model_name=row["model_name"],
            model_version=row.get("model_version"),
            metric_name=row["metric_name"],
            entity_name=row.get("entity_name"),
            severity=severity,
            baseline_value=row.get("baseline_value"),
            current_value=row.get("current_value"),
        )

        events.append(
            {
                "model_name": row["model_name"],
                "model_version": row.get("model_version"),
                "run_id": row["run_id"],
                "source_component": "drift_monitoring",
                "metric_name": row["metric_name"],
                "entity_name": row.get("entity_name"),
                "baseline_value": row.get("baseline_value"),
                "current_value": row.get("current_value"),
                "severity": severity,
                "message": message,
                "notification_status": "PENDING",
            }
        )

    return events


def persist_alert_events(
    spark,
    rows: list[Row],
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    if not rows:
        return

    schema_name, table_name = _get_alert_event_table_name(
        project=project,
        use_catalog=use_catalog,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    df = spark.createDataFrame(rows, schema=ALERT_EVENT_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def emit_alert_events_from_drift(
    spark,
    drift_rows: list[dict],
    severity_min: str = "WARNING",
    project: str = "clientes",
    use_catalog: bool = False,
) -> list[Row]:
    ctx = get_context(project=project, use_catalog=use_catalog)

    events = build_alert_events_from_drift_rows(
        drift_rows=drift_rows,
        severity_min=severity_min,
    )

    rows = [
        build_alert_event_row(
            env=ctx.env,
            project=ctx.project,
            model_name=event["model_name"],
            model_version=event["model_version"],
            run_id=event["run_id"],
            source_component=event["source_component"],
            metric_name=event["metric_name"],
            entity_name=event["entity_name"],
            baseline_value=event["baseline_value"],
            current_value=event["current_value"],
            severity=event["severity"],
            message=event["message"],
            notification_status=event["notification_status"],
        )
        for event in events
    ]

    persist_alert_events(
        spark=spark,
        rows=rows,
        project=project,
        use_catalog=use_catalog,
    )

    return rows


def determine_notification_status(
    *,
    alerting_enabled: bool,
    plan: list[dict],
) -> str:
    if not alerting_enabled:
        return "SKIPPED"
    if not plan:
        return "NO_CHANNEL"
    email_plans = [item for item in plan if item["channel"] == "email"]
    if email_plans and not email_plans[0].get("recipients"):
        return "NO_RECIPIENTS"
    return "PLANNED"


def plan_notifications_for_alert_events(
    alert_events: list[dict],
    config_path: str,
) -> list[dict]:
    alerting_config = load_alerting_config(config_path)
    alerting_enabled = bool(alerting_config.get("enable_alerting", True))

    planned = []

    for event in alert_events:
        plan = build_notification_plan(
            alerting_config=alerting_config,
            subject=f"[{event['severity']}] {event['model_name']} - {event['metric_name']}",
            message=event["message"],
        )

        status = determine_notification_status(
            alerting_enabled=alerting_enabled,
            plan=plan,
        )

        planned.append(
            {
                **event,
                "notification_plan": plan,
                "notification_status": status,
            }
        )

    return planned


def dispatch_planned_alert_events(
    pending_events: list[dict],
    config_path: str,
    secrets_resolver,
) -> list[dict]:
    alerting_config = load_alerting_config(config_path)
    dispatched = []

    for event in pending_events:
        if event.get("notification_status") != "PLANNED":
            dispatched.append({**event})
            continue

        plan = event.get("notification_plan", [])
        delivery_results = send_notification_plan(
            plan=plan,
            alerting_config=alerting_config,
            secrets_resolver=secrets_resolver,
        )

        statuses = [item["delivery_status"] for item in delivery_results]
        final_status = "FAILED" if "FAILED" in statuses else "SENT"

        delivery_errors = [
            item.get("delivery_error")
            for item in delivery_results
            if item.get("delivery_error")
        ]

        dispatched.append(
            {
                **event,
                "delivery_results": delivery_results,
                "notification_status": final_status,
                "notification_error": " | ".join(delivery_errors) if delivery_errors else None,
            }
        )

    return dispatched
