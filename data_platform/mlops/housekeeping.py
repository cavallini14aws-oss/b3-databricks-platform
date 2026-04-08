from data_platform.core.config_loader import load_yaml_config


RETENTION_MAPPING = {
    "tb_model_predictions": "tb_model_predictions_days",
    "tb_ml_alert_events": "tb_ml_alert_events_days",
    "tb_model_postprod_metrics": "tb_model_postprod_metrics_days",
    "tb_model_retraining_requests": "tb_model_retraining_requests_days",
    "tb_ml_governance_runs": "tb_ml_governance_runs_days",
    "tb_ml_smoke_runs": "tb_ml_smoke_runs_days",
}


def load_mlops_retention(config_path: str) -> dict:
    config = load_yaml_config(config_path)
    return config.get("mlops_retention", {})


def get_retention_days(
    *,
    config_path: str,
    table_name: str,
) -> int | None:
    retention_cfg = load_mlops_retention(config_path)
    key = RETENTION_MAPPING.get(table_name)
    if key is None:
        return None

    value = retention_cfg.get(key)
    return int(value) if value is not None else None


def build_retention_plan(
    *,
    config_path: str,
) -> dict[str, int]:
    retention_cfg = load_mlops_retention(config_path)

    plan = {}
    for table_name, cfg_key in RETENTION_MAPPING.items():
        value = retention_cfg.get(cfg_key)
        if value is not None:
            plan[table_name] = int(value)

    return plan
