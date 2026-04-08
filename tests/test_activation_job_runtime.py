from data_platform.core.activation_job_runtime import get_enabled_job_cycles


def test_get_enabled_job_cycles_filters_disabled_jobs(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_job_runtime.get_activation_jobs_config",
        lambda env, config_path: {
            "drift_cycle": {"enabled": True},
            "postprod_cycle": {"enabled": False},
            "retraining_cycle": {"enabled": True},
        },
    )

    result = get_enabled_job_cycles("prd")
    assert "drift_cycle" in result
    assert "retraining_cycle" in result
    assert "postprod_cycle" not in result
