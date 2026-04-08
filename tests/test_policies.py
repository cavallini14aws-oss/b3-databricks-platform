from data_platform.mlops.policies import (
    get_postprod_threshold,
    should_open_retraining_from_drift,
    should_open_retraining_from_postprod,
    should_suggest_rollback_from_postprod,
)


def test_get_postprod_threshold_returns_expected_value(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.policies.load_yaml_config",
        lambda config_path: {
            "mlops_thresholds": {
                "postprod_min_f1": 0.72,
            }
        },
    )

    threshold = get_postprod_threshold(
        config_path="config/env/dev.yml",
        metric_name="f1",
    )

    assert threshold == 0.72


def test_should_open_retraining_from_drift_respects_config(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.policies.load_yaml_config",
        lambda config_path: {
            "mlops_thresholds": {
                "auto_open_retraining_on_drift_critical": True,
            }
        },
    )

    assert should_open_retraining_from_drift(
        config_path="config/env/dev.yml",
        severity="CRITICAL",
    ) is True


def test_should_open_retraining_from_postprod_respects_threshold(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.policies.load_yaml_config",
        lambda config_path: {
            "mlops_thresholds": {
                "postprod_min_f1": 0.70,
                "auto_open_retraining_on_postprod_critical": True,
            }
        },
    )

    assert should_open_retraining_from_postprod(
        config_path="config/env/dev.yml",
        metric_name="f1",
        metric_value=0.61,
    ) is True


def test_should_suggest_rollback_from_postprod_respects_threshold(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.policies.load_yaml_config",
        lambda config_path: {
            "mlops_thresholds": {
                "postprod_min_accuracy": 0.75,
                "suggest_rollback_on_postprod_critical": True,
            }
        },
    )

    assert should_suggest_rollback_from_postprod(
        config_path="config/env/dev.yml",
        metric_name="accuracy",
        metric_value=0.60,
    ) is True
