from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import types as T

from data_platform.core.context import get_context


RETRAINING_REQUEST_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("trigger_type", T.StringType(), False),
        T.StructField("trigger_source", T.StringType(), False),
        T.StructField("trigger_severity", T.StringType(), True),
        T.StructField("reason", T.StringType(), True),
        T.StructField("request_status", T.StringType(), False),
        T.StructField("requested_by", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
    ]
)


VALID_TRIGGER_TYPES = {
    "DRIFT",
    "POSTPROD_DEGRADATION",
    "MANUAL",
}

VALID_REQUEST_STATUSES = {
    "OPEN",
    "APPROVED",
    "REJECTED",
    "EXECUTED",
}


def _get_retraining_request_table_name(
    project: str = "clientes",
    use_catalog: bool = False,
) -> tuple[str, str]:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
    table_name = ctx.naming.qualified_table(
        ctx.naming.schema_mlops,
        "tb_model_retraining_requests",
    )
    return schema_name, table_name


def validate_retraining_trigger_type(trigger_type: str) -> None:
    if trigger_type not in VALID_TRIGGER_TYPES:
        raise ValueError(
            f"Trigger type invalido: {trigger_type}. Permitidos: {sorted(VALID_TRIGGER_TYPES)}"
        )


def validate_retraining_request_status(request_status: str) -> None:
    if request_status not in VALID_REQUEST_STATUSES:
        raise ValueError(
            f"Request status invalido: {request_status}. Permitidos: {sorted(VALID_REQUEST_STATUSES)}"
        )


def persist_retraining_request(
    spark,
    *,
    model_name: str,
    model_version: str | None,
    trigger_type: str,
    trigger_source: str,
    trigger_severity: str | None,
    reason: str | None,
    request_status: str,
    requested_by: str | None,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    validate_retraining_trigger_type(trigger_type)
    validate_retraining_request_status(request_status)

    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name, table_name = _get_retraining_request_table_name(
        project=project,
        use_catalog=use_catalog,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        model_name=model_name,
        model_version=model_version,
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        trigger_severity=trigger_severity,
        reason=reason,
        request_status=request_status,
        requested_by=requested_by,
        run_id=run_id,
    )

    df = spark.createDataFrame([row], schema=RETRAINING_REQUEST_SCHEMA)

    if spark.catalog.tableExists(table_name):
        df.write.mode("append").saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def open_retraining_request(
    spark,
    *,
    model_name: str,
    model_version: str | None,
    trigger_type: str,
    trigger_source: str,
    trigger_severity: str | None,
    reason: str | None,
    requested_by: str | None,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> dict:
    persist_retraining_request(
        spark=spark,
        model_name=model_name,
        model_version=model_version,
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        trigger_severity=trigger_severity,
        reason=reason,
        request_status="OPEN",
        requested_by=requested_by,
        run_id=run_id,
        project=project,
        use_catalog=use_catalog,
    )

    return {
        "model_name": model_name,
        "model_version": model_version,
        "trigger_type": trigger_type,
        "trigger_source": trigger_source,
        "trigger_severity": trigger_severity,
        "reason": reason,
        "request_status": "OPEN",
        "requested_by": requested_by,
        "run_id": run_id,
    }
