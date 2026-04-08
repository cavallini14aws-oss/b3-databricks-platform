from data_platform.mlops.housekeeping import load_mlops_retention_from_activation_control
from data_platform.mlops.notifications import load_alerting_config_from_activation_control
from data_platform.mlops.policies import (
    get_postprod_threshold_from_activation_control,
    load_mlops_thresholds_from_activation_control,
)


def test_load_alerting_config_from_activation_control(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.notifications.get_activation_notifications_config",
        lambda env, config_path: {
            "enabled": True,
            "severity_min": "WARNING",
            "email": {
                "enabled": True,
                "recipients": ["a@test.com"],
                "smtp": {
                    "scope": "scope-a",
                    "host_key": "SMTP_HOST",
                    "port_key": "SMTP_PORT",
                    "username_key": "SMTP_USERNAME",
                    "password_key": "SMTP_PASSWORD",
                },
            },
            "slack": {
                "enabled": True,
                "webhook": {
                    "scope": "scope-a",
                    "key": "SLACK_WEBHOOK",
                },
            },
            "teams": {
                "enabled": False,
                "webhook": {
                    "scope": "scope-a",
                    "key": "TEAMS_WEBHOOK",
                },
            },
        },
    )

    cfg = load_alerting_config_from_activation_control(env="dev")

    assert cfg["enable_alerting"] is True
    assert cfg["email_enabled"] is True
    assert cfg["slack_enabled"] is True
    assert cfg["recipients"] == "a@test.com"


def test_load_mlops_thresholds_from_activation_control(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.policies.get_activation_thresholds_config",
        lambda env, config_path: {
            "postprod_min_f1": 0.71,
        },
    )

    cfg = load_mlops_thresholds_from_activation_control(env="prd")
    assert cfg["postprod_min_f1"] == 0.71


def test_get_postprod_threshold_from_activation_control(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.policies.get_activation_thresholds_config",
        lambda env, config_path: {
            "postprod_min_accuracy": 0.75,
        },
    )

    threshold = get_postprod_threshold_from_activation_control(
        env="prd",
        metric_name="accuracy",
    )

    assert threshold == 0.75


def test_load_mlops_retention_from_activation_control(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.housekeeping.get_activation_retention_config",
        lambda env, config_path: {
            "tb_model_predictions_days": 90,
        },
    )

    cfg = load_mlops_retention_from_activation_control(env="prd")
    assert cfg["tb_model_predictions_days"] == 90
