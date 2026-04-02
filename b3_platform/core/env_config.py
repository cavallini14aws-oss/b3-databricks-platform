from dataclasses import dataclass

from b3_platform.core.config_loader import load_yaml_config
from b3_platform.core.env import get_env


@dataclass(frozen=True)
class EnvironmentConfig:
    env_name: str
    use_catalog: bool
    debug_mode: bool
    model_artifact_base_path: str
    enable_model_artifact_persistence: bool
    flags: dict


def load_environment_config(env: str | None = None) -> EnvironmentConfig:
    resolved_env = env or get_env()
    config_path = f"config/env/{resolved_env}.yml"

    config = load_yaml_config(config_path)

    environment = config.get("environment", {})
    storage = config.get("storage", {})
    flags = config.get("flags", {})

    return EnvironmentConfig(
        env_name=environment.get("name", resolved_env),
        use_catalog=bool(environment.get("use_catalog", False)),
        debug_mode=bool(environment.get("debug_mode", False)),
        model_artifact_base_path=storage.get("model_artifact_base_path", "/tmp/local_models"),
        enable_model_artifact_persistence=bool(
            storage.get("enable_model_artifact_persistence", False)
        ),
        flags=flags,
    )
