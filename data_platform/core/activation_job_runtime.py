from data_platform.core.activation_control import get_activation_jobs_config


def get_enabled_job_cycles(
    env: str,
    config_path: str = "config/activation/operational_control.yml",
) -> dict:
    jobs_cfg = get_activation_jobs_config(env=env, config_path=config_path)
    return {
        job_name: job_cfg
        for job_name, job_cfg in jobs_cfg.items()
        if job_cfg.get("enabled", False)
    }
