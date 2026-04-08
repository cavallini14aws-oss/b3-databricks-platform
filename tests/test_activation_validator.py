from data_platform.core.activation_validator import validate_activation_environment


def test_validate_activation_environment_returns_errors_when_missing_fields(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_validator.get_activation_environment_config",
        lambda env, config_path: {
            "databricks": {},
            "notifications": {
                "enabled": True,
                "email": {"enabled": True, "recipients": []},
            },
            "jobs": {
                "drift_cycle": {"enabled": True, "cron": ""},
            },
            "go_live": {"blockers": []},
        },
    )

    result = validate_activation_environment("dev")

    assert result["ready"] is False
    assert len(result["errors"]) > 0


def test_validate_activation_environment_returns_warning_for_pending_ready_flags(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_validator.get_activation_environment_config",
        lambda env, config_path: {
            "databricks": {
                "workspace_host_key": "DEV_WORKSPACE_HOST",
                "databricks_token_key": "DEV_DATABRICKS_TOKEN",
                "cluster_id_key": "DEV_CLUSTER_ID",
                "secret_scope": "keyvault-dev-datahub",
            },
            "notifications": {
                "enabled": True,
                "severity_min": "WARNING",
                "email": {
                    "enabled": True,
                    "recipients": ["mlops-dev@empresa.com"],
                    "smtp": {
                        "scope": "keyvault-dev-datahub",
                        "host_key": "SMTP_HOST",
                        "port_key": "SMTP_PORT",
                        "username_key": "SMTP_USERNAME",
                        "password_key": "SMTP_PASSWORD",
                        "ready": False,
                    },
                },
            },
            "jobs": {
                "drift_cycle": {"enabled": True, "cron": "0 0 8 * * ?"},
            },
            "go_live": {"blockers": ["secrets_pendentes"]},
        },
    )

    result = validate_activation_environment("dev")

    assert result["ready"] is True
    assert len(result["warnings"]) > 0
