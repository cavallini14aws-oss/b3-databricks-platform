from dataclasses import dataclass

from b3_platform.core.config_loader import load_yaml_config


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
