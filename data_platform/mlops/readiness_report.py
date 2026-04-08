from data_platform.core.context import get_context


READINESS_ITEMS = [
    ("model_registry", "tb_model_registry"),
    ("model_deployments", "tb_model_deployments"),
    ("model_evaluation", "tb_model_evaluation"),
    ("confusion_matrix", "tb_model_confusion_matrix"),
    ("model_predictions", "tb_model_predictions"),
    ("prediction_monitoring", "tb_model_prediction_monitoring"),
    ("feature_monitoring", "tb_model_feature_monitoring"),
    ("drift_monitoring", "tb_model_drift_monitoring"),
    ("alert_events", "tb_ml_alert_events"),
    ("smoke_runs", "tb_ml_smoke_runs"),
    ("governance_runs", "tb_ml_governance_runs"),
    ("postprod_metrics", "tb_model_postprod_metrics"),
    ("retraining_requests", "tb_model_retraining_requests"),
]


def build_mlops_readiness_report(
    spark,
    *,
    project: str = "clientes",
    use_catalog: bool = False,
) -> dict:
    ctx = get_context(project=project, use_catalog=use_catalog)

    checks = {}
    ready_count = 0

    for check_name, table_suffix in READINESS_ITEMS:
        table_name = ctx.naming.qualified_table(ctx.naming.schema_mlops, table_suffix)
        exists = bool(spark.catalog.tableExists(table_name))
        checks[check_name] = {
            "table_name": table_name,
            "exists": exists,
        }
        if exists:
            ready_count += 1

    total_count = len(READINESS_ITEMS)
    readiness_score = round(ready_count / total_count, 4) if total_count else 0.0

    return {
        "project": ctx.project,
        "env": ctx.env,
        "schema_mlops": ctx.naming.qualified_schema(ctx.naming.schema_mlops),
        "ready_items": ready_count,
        "total_items": total_count,
        "readiness_score": readiness_score,
        "checks": checks,
    }
