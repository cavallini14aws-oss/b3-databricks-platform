from dataclasses import dataclass

from data_platform.core.config_loader import load_yaml_config


@dataclass(frozen=True)
class CiProvider:
    name: str
    enabled: bool
    display_name: str


def load_ci_providers() -> list[CiProvider]:
    config = load_yaml_config("config/ci_providers.yml")
    providers = config.get("providers", {})

    result = []
    for name, payload in providers.items():
        result.append(
            CiProvider(
                name=name,
                enabled=bool(payload.get("enabled", False)),
                display_name=payload.get("display_name", name),
            )
        )

    return result


def get_enabled_ci_providers() -> list[CiProvider]:
    return [provider for provider in load_ci_providers() if provider.enabled]


def get_active_ci_provider() -> CiProvider:
    enabled = get_enabled_ci_providers()

    if not enabled:
        raise ValueError("Nenhum provider de CI/CD está habilitado em config/ci_providers.yml")

    if len(enabled) > 1:
        names = [provider.name for provider in enabled]
        raise ValueError(
            f"Mais de um provider habilitado ao mesmo tempo: {names}. "
            "A plataforma deve ter apenas um provider ativo por vez."
        )

    return enabled[0]
