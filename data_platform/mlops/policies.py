from data_platform.core.config_loader import load_yaml_config


def load_mlops_thresholds(config_path: str) -> dict:
    config = load_yaml_config(config_path)
    return config.get("mlops_thresholds", {})


def get_postprod_threshold(
    *,
    config_path: str,
    metric_name: str,
) -> float | None:
    thresholds = load_mlops_thresholds(config_path)

    mapping = {
        "accuracy": "postprod_min_accuracy",
        "f1": "postprod_min_f1",
        "precision": "postprod_min_precision",
        "recall": "postprod_min_recall",
    }

    key = mapping.get(metric_name)
    if key is None:
        return None

    value = thresholds.get(key)
    return float(value) if value is not None else None


def should_open_retraining_from_drift(
    *,
    config_path: str,
    severity: str,
) -> bool:
    thresholds = load_mlops_thresholds(config_path)
    return bool(
        thresholds.get("auto_open_retraining_on_drift_critical", False)
    ) and severity == "CRITICAL"


def should_open_retraining_from_postprod(
    *,
    config_path: str,
    metric_name: str,
    metric_value: float,
) -> bool:
    threshold = get_postprod_threshold(
        config_path=config_path,
        metric_name=metric_name,
    )
    if threshold is None:
        return False

    thresholds = load_mlops_thresholds(config_path)
    enabled = bool(
        thresholds.get("auto_open_retraining_on_postprod_critical", False)
    )

    return enabled and metric_value < threshold


def should_suggest_rollback_from_postprod(
    *,
    config_path: str,
    metric_name: str,
    metric_value: float,
) -> bool:
    threshold = get_postprod_threshold(
        config_path=config_path,
        metric_name=metric_name,
    )
    if threshold is None:
        return False

    thresholds = load_mlops_thresholds(config_path)
    enabled = bool(
        thresholds.get("suggest_rollback_on_postprod_critical", False)
    )

    return enabled and metric_value < threshold
