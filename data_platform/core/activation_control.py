from data_platform.core.config_loader import load_yaml_config
from data_platform.core.env import get_env


DEFAULT_ACTIVATION_CONTROL_PATH = "config/activation/operational_control.yml"


def load_activation_control(config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH) -> dict:
    return load_yaml_config(config_path)


def get_activation_environment_config(
    env: str | None = None,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    resolved_env = env or get_env()
    control = load_activation_control(config_path)
    environments = control.get("environments", {})
    env_cfg = environments.get(resolved_env)

    if env_cfg is None:
        raise ValueError(f"Ambiente não encontrado no activation control: {resolved_env}")

    return env_cfg


def get_activation_notifications_config(
    env: str | None = None,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    env_cfg = get_activation_environment_config(env=env, config_path=config_path)
    return env_cfg.get("notifications", {})


def get_activation_retraining_config(
    env: str | None = None,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    env_cfg = get_activation_environment_config(env=env, config_path=config_path)
    return env_cfg.get("retraining", {})


def get_activation_thresholds_config(
    env: str | None = None,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    env_cfg = get_activation_environment_config(env=env, config_path=config_path)
    return env_cfg.get("thresholds", {})


def get_activation_retention_config(
    env: str | None = None,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    env_cfg = get_activation_environment_config(env=env, config_path=config_path)
    return env_cfg.get("retention", {})


def get_activation_jobs_config(
    env: str | None = None,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    env_cfg = get_activation_environment_config(env=env, config_path=config_path)
    return env_cfg.get("jobs", {})


def get_activation_databricks_config(
    env: str | None = None,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    env_cfg = get_activation_environment_config(env=env, config_path=config_path)
    return env_cfg.get("databricks", {})
