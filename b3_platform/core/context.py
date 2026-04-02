from dataclasses import dataclass

from b3_platform.core.env import get_env
from b3_platform.core.env_config import EnvironmentConfig, load_environment_config
from b3_platform.core.flags import PlatformFlags
from b3_platform.core.naming import Naming


@dataclass(frozen=True)
class PlatformContext:
    env: str
    project: str
    naming: Naming
    flags: PlatformFlags
    env_config: EnvironmentConfig

    @property
    def secret_scope(self) -> str:
        return f"keyvault-{self.env}-datahub"

    @property
    def debug_mode(self) -> bool:
        return self.env_config.debug_mode

    @property
    def model_artifact_base_path(self) -> str:
        return self.env_config.model_artifact_base_path

    @property
    def enable_model_artifact_persistence(self) -> bool:
        return self.env_config.enable_model_artifact_persistence


def get_context(
    project: str = "clientes",
    use_catalog: bool | None = None,
    flags: PlatformFlags | None = None,
) -> PlatformContext:
    env = get_env()
    env_config = load_environment_config(env)

    resolved_use_catalog = env_config.use_catalog if use_catalog is None else use_catalog
    resolved_flags = flags or PlatformFlags.from_dict(env_config.flags)

    naming = Naming(
        env=env,
        project=project,
        use_catalog=resolved_use_catalog,
    )

    return PlatformContext(
        env=env,
        project=project,
        naming=naming,
        flags=resolved_flags,
        env_config=env_config,
    )
