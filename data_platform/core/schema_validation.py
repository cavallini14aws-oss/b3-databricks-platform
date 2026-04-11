from pathlib import Path


REQUIRED_SCHEMA_SPECS = [
    "sql/create_tables/mlops/001_create_tb_model_postprod_metrics.sql",
    "sql/create_tables/mlops/002_create_tb_ml_alert_events.sql",
    "sql/create_tables/mlops/003_create_tb_model_retraining_requests.sql",
]


def validate_required_schema_specs() -> dict:
    missing = [path for path in REQUIRED_SCHEMA_SPECS if not Path(path).exists()]

    return {
        "valid": len(missing) == 0,
        "required_specs": list(REQUIRED_SCHEMA_SPECS),
        "missing_specs": missing,
    }
