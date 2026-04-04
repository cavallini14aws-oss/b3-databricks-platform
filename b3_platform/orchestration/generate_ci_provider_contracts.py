import argparse
from pathlib import Path

from b3_platform.orchestration.ci_provider_config import load_ci_providers
from b3_platform.orchestration.ci_secrets_contract import get_provider_all_secrets


PROVIDER_GUIDANCE = {
    "github_actions": {
        "environments": ["dev", "hml", "prd"],
        "approval_model": {
            "dev": "sem required reviewers",
            "hml": "reviewer opcional ou gate simples",
            "prd": "required reviewers obrigatório e impedir self-review",
        },
        "notes": [
            "Usar GitHub Environments para dev, hml e prd.",
            "Segregar secrets por environment.",
            "PRD deve usar credencial isolada de produção.",
        ],
    },
    "azure_devops": {
        "environments": ["databricks-dev", "databricks-hml", "databricks-prd"],
        "approval_model": {
            "dev": "deploy automático ou semi-automático",
            "hml": "approval opcional conforme cliente",
            "prd": "Approvals and checks obrigatório no environment de PRD",
        },
        "notes": [
            "Usar Environments no Azure DevOps.",
            "Usar variable groups ou secret variables segregadas por ambiente.",
            "PRD deve usar credencial isolada de produção.",
        ],
    },
    "bitbucket": {
        "environments": ["development", "staging", "production"],
        "approval_model": {
            "dev": "deploy controlado pela branch main",
            "hml": "trigger manual recomendado",
            "prd": "trigger manual + branch permissions + merge checks",
        },
        "notes": [
            "Usar deployment environments do Bitbucket.",
            "Usar secured variables por deployment.",
            "PRD deve depender de merge controlado e permissão restrita.",
        ],
    },
    "aws": {
        "environments": ["dev", "hml", "prd"],
        "approval_model": {
            "dev": "deploy automático ou via stage inicial",
            "hml": "approval opcional em stage intermediário",
            "prd": "approval obrigatório no CodePipeline antes do deploy",
        },
        "notes": [
            "Usar CodePipeline/CodeBuild como adaptador.",
            "Usar Secrets Manager ou Parameter Store.",
            "PRD deve usar credencial isolada de produção.",
        ],
    },
}


def build_contract_markdown(provider_name: str, enabled: bool) -> str:
    secrets_by_env = get_provider_all_secrets(provider_name)
    guidance = PROVIDER_GUIDANCE[provider_name]

    lines = []
    lines.append(f"# {provider_name}")
    lines.append("")
    lines.append(f"- enabled: {'true' if enabled else 'false'}")
    lines.append("")
    lines.append("## Environments esperados")
    for env in guidance["environments"]:
        lines.append(f"- {env}")
    lines.append("")
    lines.append("## Secrets obrigatórios por ambiente")
    for env, data in secrets_by_env.items():
        lines.append(f"### {env}")
        for item in data.required:
            lines.append(f"- {item}")
        lines.append("")
    lines.append("## Modelo de aprovação recomendado")
    for env, rule in guidance["approval_model"].items():
        lines.append(f"- {env}: {rule}")
    lines.append("")
    lines.append("## Fluxo recomendado")
    lines.append("1. validate")
    lines.append("2. deploy dev")
    lines.append("3. deploy hml")
    lines.append("4. deploy prd")
    lines.append("")
    lines.append("## Observações")
    for note in guidance["notes"]:
        lines.append(f"- {note}")
    lines.append("")

    return "\n".join(lines)


def generate_provider_contracts(write_all: bool = True) -> list[str]:
    providers = load_ci_providers()
    written = []

    for provider in providers:
        if not write_all and not provider.enabled:
            continue

        output_path = Path(f"ci_adapters/{provider.name}/CONTRACT.md")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            build_contract_markdown(provider.name, provider.enabled),
            encoding="utf-8",
        )
        written.append(str(output_path))

    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera CONTRACT.md por provider de CI/CD."
    )
    parser.add_argument(
        "--write-all",
        default="true",
        choices=["true", "false"],
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    write_all = parsed.write_all.lower() == "true"
    files = generate_provider_contracts(write_all=write_all)

    print("Contracts gerados:")
    for file_path in files:
        print(file_path)


if __name__ == "__main__":
    main()
