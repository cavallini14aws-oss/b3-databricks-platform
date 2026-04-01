from dataclasses import dataclass

from b3_platform.core.env import get_env
from b3_platform.core.flags import PlatformFlags
from b3_platform.core.naming import Naming


@dataclass(frozen=True)
class PlatformContext:
    env: str
    project: str
    naming: Naming
    flags: PlatformFlags

    @property
    def secret_scope(self) -> str:
        return f"keyvault-{self.env}-datahub"


def get_context(
    project: str = "clientes",
    use_catalog: bool = False,
    flags: PlatformFlags | None = None,
) -> PlatformContext:
    env = get_env()
    naming = Naming(env=env, project=project, use_catalog=use_catalog)

    return PlatformContext(
        env=env,
        project=project,
        naming=naming,
        flags=flags or PlatformFlags.default(),
    )
