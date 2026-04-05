import argparse
from pathlib import Path

from data_platform.orchestration.ci_provider_config import get_active_ci_provider
from data_platform.orchestration.ci_secrets_contract import get_provider_all_secrets


def build_env_example(provider_name: str) -> str:
    secrets = get_provider_all_secrets(provider_name)

    lines = []
    lines.append(f"# Active provider: {provider_name}")
    lines.append("# Preencher os valores abaixo conforme o ambiente real")
    lines.append("")

    for env, data in secrets.items():
        lines.append(f"# {env}")
        for item in data.required:
            lines.append(f"{item}=")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def build_markdown(provider_name: str) -> str:
    secrets = get_provider_all_secrets(provider_name)

    lines = []
    lines.append(f"# Secrets bootstrap - {provider_name}")
    lines.append("")
    lines.append("## Objetivo")
    lines.append("Preencher os secrets obrigatórios do provider ativo para habilitar deploy real.")
    lines.append("")
    lines.append("## Secrets por ambiente")

    for env, data in secrets.items():
        lines.append(f"### {env}")
        for item in data.required:
            lines.append(f"- {item}")
        lines.append("")

    lines.append("## Como usar")
    lines.append("1. preencher os valores reais")
    lines.append("2. configurar os secrets no provider ativo")
    lines.append("3. rodar `python -m data_platform.orchestration.validate_active_ci_provider`")
    lines.append("4. rodar `python -m data_platform.orchestration.show_platform_readiness_report`")
    lines.append("")

    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera bootstrap de secrets para o provider ativo."
    )
    parser.add_argument(
        "--env-output",
        default="artifacts/active_provider_secrets.env.example",
    )
    parser.add_argument(
        "--md-output",
        default="artifacts/active_provider_secrets_bootstrap.md",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    active_provider = get_active_ci_provider()

    env_text = build_env_example(active_provider.name)
    md_text = build_markdown(active_provider.name)

    env_output = Path(parsed.env_output)
    md_output = Path(parsed.md_output)

    env_output.parent.mkdir(parents=True, exist_ok=True)
    md_output.parent.mkdir(parents=True, exist_ok=True)

    env_output.write_text(env_text, encoding="utf-8")
    md_output.write_text(md_text, encoding="utf-8")

    print(f"Provider ativo: {active_provider.name}")
    print(f"ENV example: {env_output}")
    print(f"Markdown bootstrap: {md_output}")


if __name__ == "__main__":
    main()
