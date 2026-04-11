from data_platform.core.job_config import load_job_runtime_from_activation_control


def test_load_job_runtime_from_activation_control(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.job_config.get_activation_databricks_config",
        lambda env, config_path: {
            "enabled": True,
            "use_catalog": True,
        },
    )
    monkeypatch.setattr(
        "data_platform.core.job_config.get_activation_jobs_config",
        lambda env, config_path: {
            "drift_cycle": {"enabled": True},
        },
    )

    result = load_job_runtime_from_activation_control(env="prd")

    assert result["databricks"]["enabled"] is True
    assert result["databricks"]["use_catalog"] is True
    assert result["jobs"]["drift_cycle"]["enabled"] is True
