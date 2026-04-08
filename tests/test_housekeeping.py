from data_platform.mlops.housekeeping import (
    build_retention_plan,
    get_retention_days,
)


def test_get_retention_days_returns_expected_value(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.housekeeping.load_yaml_config",
        lambda config_path: {
            "mlops_retention": {
                "tb_model_predictions_days": 90,
            }
        },
    )

    value = get_retention_days(
        config_path="config/env/dev.yml",
        table_name="tb_model_predictions",
    )

    assert value == 90


def test_get_retention_days_returns_none_for_unknown_table(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.housekeeping.load_yaml_config",
        lambda config_path: {
            "mlops_retention": {}
        },
    )

    value = get_retention_days(
        config_path="config/env/dev.yml",
        table_name="tb_unknown",
    )

    assert value is None


def test_build_retention_plan_returns_expected_shape(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.housekeeping.load_yaml_config",
        lambda config_path: {
            "mlops_retention": {
                "tb_model_predictions_days": 90,
                "tb_ml_alert_events_days": 180,
            }
        },
    )

    plan = build_retention_plan(
        config_path="config/env/dev.yml",
    )

    assert plan["tb_model_predictions"] == 90
    assert plan["tb_ml_alert_events"] == 180
