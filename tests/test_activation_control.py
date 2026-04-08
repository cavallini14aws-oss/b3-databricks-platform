from data_platform.core.activation_control import (
    get_activation_environment_config,
    get_activation_jobs_config,
    get_activation_notifications_config,
)


def test_get_activation_environment_config_returns_expected_env(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_control.load_yaml_config",
        lambda path: {
            "environments": {
                "dev": {
                    "notifications": {"enabled": True},
                    "jobs": {"drift_cycle": {"enabled": True}},
                }
            }
        },
    )

    env_cfg = get_activation_environment_config("dev")
    assert env_cfg["notifications"]["enabled"] is True


def test_get_activation_notifications_config_returns_expected_block(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_control.load_yaml_config",
        lambda path: {
            "environments": {
                "dev": {
                    "notifications": {"enabled": True, "severity_min": "WARNING"},
                }
            }
        },
    )

    cfg = get_activation_notifications_config("dev")
    assert cfg["severity_min"] == "WARNING"


def test_get_activation_jobs_config_returns_expected_block(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_control.load_yaml_config",
        lambda path: {
            "environments": {
                "dev": {
                    "jobs": {"drift_cycle": {"enabled": True}},
                }
            }
        },
    )

    cfg = get_activation_jobs_config("dev")
    assert cfg["drift_cycle"]["enabled"] is True
