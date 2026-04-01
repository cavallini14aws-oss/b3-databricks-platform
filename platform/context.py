from dataclasses import dataclass

from platform.env import get_env
from platform.flags import PlatformFlags
from platform.naming import Naming


@dataclass(frozen=True)
class PlatformContext:
    env: str
    project: str
    naming: Naming
    flags: PlatformFlags

    @property
    def secret_scope(self) -> str:
        return f"keyvault-{self.env}-datahub"


def get_context(project: str = "clientes") -> PlatformContext:
    env = get_env()
    naming = Naming(env=env, project=project)
    flags = PlatformFlags()

    return PlatformContext(
        env=env,
        project=project,
        naming=naming,
        flags=flags,
    )
