from data_platform.core.job_config import load_job_config


def test_activation_control_overrides_runtime_operational_fields(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.job_config._load_legacy_job_yaml",
        lambda env: {
            "job": {
                "environment": env,
                "cluster_key": f"{env}-legacy-cluster",
                "cluster_mode": "existing_or_job_cluster",
                "workspace_root": "/Workspace/Repos/legacy",
                "default_timeout_seconds": 1111,
                "max_retries": 9,
                "use_catalog": False,
                "default_config_path": "config/legacy.yml",
            },
            "promotion": {"rules": []},
            "quality_gates": {"ml": {}},
        },
    )

    monkeypatch.setattr(
        "data_platform.core.job_config.get_activation_databricks_config",
        lambda env, config_path: {
            "enabled": True,
            "use_catalog": True,
            "workspace_root": "/Workspace/Repos/from-activation",
            "default_config_path": "config/from-activation.yml",
        },
    )

    monkeypatch.setattr(
        "data_platform.core.job_config.get_activation_jobs_config",
        lambda env, config_path: {
            "operational_cycle": {
                "timeout_seconds": 2222,
                "retries": 3,
            }
        },
    )

    cfg = load_job_config("prd")

    assert cfg.cluster_key == "prd-legacy-cluster"
    assert cfg.use_catalog is True
    assert cfg.workspace_root == "/Workspace/Repos/from-activation"
    assert cfg.default_config_path == "config/from-activation.yml"
    assert cfg.default_timeout_seconds == 2222
    assert cfg.max_retries == 3
