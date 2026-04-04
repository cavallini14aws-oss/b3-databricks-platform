from dataclasses import dataclass

from b3_platform.core.config_loader import load_yaml_config


@dataclass(frozen=True)
class ProviderEnvironmentSecrets:
    provider: str
    environment: str
    required: list[str]


def load_ci_secrets_contract() -> dict:
    return load_yaml_config("config/ci_secrets_contract.yml")


def get_provider_environment_secrets(
    provider_name: str,
    environment: str,
) -> ProviderEnvironmentSecrets:
    config = load_ci_secrets_contract()
    providers = config.get("providers", {})

    if provider_name not in providers:
        raise ValueError(f"Provider não encontrado no contrato: {provider_name}")

    provider_payload = providers[provider_name]
    if environment not in provider_payload:
        raise ValueError(
            f"Environment {environment} não encontrado para provider {provider_name}"
        )

    required = provider_payload[environment].get("required", [])

    return ProviderEnvironmentSecrets(
        provider=provider_name,
        environment=environment,
        required=required,
    )


def get_provider_all_secrets(provider_name: str) -> dict[str, ProviderEnvironmentSecrets]:
    environments = ["dev", "hml", "prd"]
    return {
        env: get_provider_environment_secrets(provider_name, env)
        for env in environments
    }
