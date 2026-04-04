import argparse
import json
from pathlib import Path

from b3_platform.orchestration.ci_provider_config import (
    get_active_ci_provider,
    load_ci_providers,
)
from b3_platform.orchestration.ci_secrets_contract import get_provider_all_secrets


GITHUB_ACTIONS_TEMPLATE = """name: Databricks CI

on:
  pull_request:
  push:
    branches: [ "main" ]

jobs:
  validate:
    runs-on: ubuntu-latest
    env:
      DEV_WORKSPACE_HOST: ${{ secrets.DEV_WORKSPACE_HOST }}
      DEV_DATABRICKS_TOKEN: ${{ secrets.DEV_DATABRICKS_TOKEN }}
      DEV_CLUSTER_ID: ${{ secrets.DEV_CLUSTER_ID }}
      HML_WORKSPACE_HOST: ${{ secrets.HML_WORKSPACE_HOST }}
      HML_DATABRICKS_TOKEN: ${{ secrets.HML_DATABRICKS_TOKEN }}
      HML_CLUSTER_ID: ${{ secrets.HML_CLUSTER_ID }}
      PRD_WORKSPACE_HOST: ${{ secrets.PRD_WORKSPACE_HOST }}
      PRD_DATABRICKS_TOKEN: ${{ secrets.PRD_DATABRICKS_TOKEN }}
      PRD_CLUSTER_ID: ${{ secrets.PRD_CLUSTER_ID }}

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Compile Python
        run: python -m compileall b3_platform config pipelines resources sql

      - name: Validate active CI provider secrets contract
        run: python -m b3_platform.orchestration.validate_active_ci_provider

      - name: Generate platform artifacts
        run: |
          python -m b3_platform.flow_specs.generate_registry
          python -m b3_platform.flow_specs.generate_jobs --environment dev
          python -m b3_platform.flow_specs.generate_jobs --environment hml
          python -m b3_platform.flow_specs.generate_jobs --environment prd
          python -m b3_platform.flow_specs.generate_resources --environment dev
          python -m b3_platform.flow_specs.generate_resources --environment hml
          python -m b3_platform.flow_specs.generate_resources --environment prd
          python -m b3_platform.flow_specs.generate_databricks_bundle_root
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd

      - name: Set up Databricks CLI
        run: pip install databricks-cli

      - name: Validate bundle on DEV target
        env:
          DATABRICKS_HOST: ${{ secrets.DEV_WORKSPACE_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DEV_DATABRICKS_TOKEN }}
        run: databricks bundle validate -t dev

  deploy-dev:
    if: github.ref == 'refs/heads/main'
    needs: validate
    runs-on: ubuntu-latest
    environment: dev

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Set up Databricks CLI
        run: pip install databricks-cli

      - name: Generate bundle artifacts
        run: |
          python -m b3_platform.flow_specs.generate_databricks_bundle_root
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd

      - name: Deploy DEV
        env:
          DATABRICKS_HOST: ${{ secrets.DEV_WORKSPACE_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DEV_DATABRICKS_TOKEN }}
          DEV_CLUSTER_ID: ${{ secrets.DEV_CLUSTER_ID }}
        run: databricks bundle deploy -t dev

  deploy-hml:
    if: github.ref == 'refs/heads/main'
    needs: deploy-dev
    runs-on: ubuntu-latest
    environment: hml

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Set up Databricks CLI
        run: pip install databricks-cli

      - name: Generate bundle artifacts
        run: |
          python -m b3_platform.flow_specs.generate_databricks_bundle_root
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd

      - name: Deploy HML
        env:
          DATABRICKS_HOST: ${{ secrets.HML_WORKSPACE_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.HML_DATABRICKS_TOKEN }}
          HML_CLUSTER_ID: ${{ secrets.HML_CLUSTER_ID }}
        run: databricks bundle deploy -t hml

  deploy-prd:
    if: github.ref == 'refs/heads/main'
    needs: deploy-hml
    runs-on: ubuntu-latest
    environment: prd

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Set up Databricks CLI
        run: pip install databricks-cli

      - name: Generate bundle artifacts
        run: |
          python -m b3_platform.flow_specs.generate_databricks_bundle_root
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
          python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd

      - name: Deploy PRD
        env:
          DATABRICKS_HOST: ${{ secrets.PRD_WORKSPACE_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PRD_DATABRICKS_TOKEN }}
          PRD_CLUSTER_ID: ${{ secrets.PRD_CLUSTER_ID }}
        run: databricks bundle deploy -t prd
"""

AZURE_DEVOPS_TEMPLATE = """trigger:
  branches:
    include:
      - main

pr:
  branches:
    include:
      - "*"

stages:
  - stage: Validate
    jobs:
      - job: validate_bundle
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - script: python -m compileall b3_platform config pipelines resources sql
          - script: |
              python -m b3_platform.flow_specs.generate_registry
              python -m b3_platform.flow_specs.generate_jobs --environment dev
              python -m b3_platform.flow_specs.generate_jobs --environment hml
              python -m b3_platform.flow_specs.generate_jobs --environment prd
              python -m b3_platform.flow_specs.generate_resources --environment dev
              python -m b3_platform.flow_specs.generate_resources --environment hml
              python -m b3_platform.flow_specs.generate_resources --environment prd
              python -m b3_platform.flow_specs.generate_databricks_bundle_root
              python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
              python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
              python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
          - script: pip install databricks-cli
          - script: databricks bundle validate -t dev
            env:
              DATABRICKS_HOST: $(DEV_WORKSPACE_HOST)
              DATABRICKS_TOKEN: $(DEV_DATABRICKS_TOKEN)

  - stage: DeployDev
    dependsOn: Validate
    jobs:
      - deployment: deploy_dev
        environment: databricks-dev
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                - script: pip install databricks-cli
                - script: |
                    python -m b3_platform.flow_specs.generate_databricks_bundle_root
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
                - script: databricks bundle deploy -t dev
                  env:
                    DATABRICKS_HOST: $(DEV_WORKSPACE_HOST)
                    DATABRICKS_TOKEN: $(DEV_DATABRICKS_TOKEN)
                    DEV_CLUSTER_ID: $(DEV_CLUSTER_ID)

  - stage: DeployHml
    dependsOn: DeployDev
    jobs:
      - deployment: deploy_hml
        environment: databricks-hml
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                - script: pip install databricks-cli
                - script: |
                    python -m b3_platform.flow_specs.generate_databricks_bundle_root
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
                - script: databricks bundle deploy -t hml
                  env:
                    DATABRICKS_HOST: $(HML_WORKSPACE_HOST)
                    DATABRICKS_TOKEN: $(HML_DATABRICKS_TOKEN)
                    HML_CLUSTER_ID: $(HML_CLUSTER_ID)

  - stage: DeployPrd
    dependsOn: DeployHml
    jobs:
      - deployment: deploy_prd
        environment: databricks-prd
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                - script: pip install databricks-cli
                - script: |
                    python -m b3_platform.flow_specs.generate_databricks_bundle_root
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
                    python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
                - script: databricks bundle deploy -t prd
                  env:
                    DATABRICKS_HOST: $(PRD_WORKSPACE_HOST)
                    DATABRICKS_TOKEN: $(PRD_DATABRICKS_TOKEN)
                    PRD_CLUSTER_ID: $(PRD_CLUSTER_ID)
"""

BITBUCKET_TEMPLATE = """image: python:3.12

pipelines:
  pull-requests:
    "**":
      - step:
          name: Validate bundle artifacts
          caches:
            - pip
          script:
            - python -m compileall b3_platform config pipelines resources sql
            - python -m b3_platform.flow_specs.generate_registry
            - python -m b3_platform.flow_specs.generate_jobs --environment dev
            - python -m b3_platform.flow_specs.generate_jobs --environment hml
            - python -m b3_platform.flow_specs.generate_jobs --environment prd
            - python -m b3_platform.flow_specs.generate_resources --environment dev
            - python -m b3_platform.flow_specs.generate_resources --environment hml
            - python -m b3_platform.flow_specs.generate_resources --environment prd
            - python -m b3_platform.flow_specs.generate_databricks_bundle_root
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
            - pip install databricks-cli
            - databricks bundle validate -t dev

  branches:
    main:
      - step:
          name: Deploy DEV
          deployment: development
          caches:
            - pip
          script:
            - python -m b3_platform.flow_specs.generate_databricks_bundle_root
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
            - pip install databricks-cli
            - databricks bundle deploy -t dev

      - step:
          name: Deploy HML
          deployment: staging
          trigger: manual
          caches:
            - pip
          script:
            - python -m b3_platform.flow_specs.generate_databricks_bundle_root
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
            - pip install databricks-cli
            - databricks bundle deploy -t hml

      - step:
          name: Deploy PRD
          deployment: production
          trigger: manual
          caches:
            - pip
          script:
            - python -m b3_platform.flow_specs.generate_databricks_bundle_root
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
            - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
            - pip install databricks-cli
            - databricks bundle deploy -t prd
"""

AWS_TEMPLATE = """version: 0.2

phases:
  install:
    commands:
      - pip install databricks-cli
  pre_build:
    commands:
      - python -m compileall b3_platform config pipelines resources sql
      - python -m b3_platform.flow_specs.generate_registry
      - python -m b3_platform.flow_specs.generate_jobs --environment dev
      - python -m b3_platform.flow_specs.generate_jobs --environment hml
      - python -m b3_platform.flow_specs.generate_jobs --environment prd
      - python -m b3_platform.flow_specs.generate_resources --environment dev
      - python -m b3_platform.flow_specs.generate_resources --environment hml
      - python -m b3_platform.flow_specs.generate_resources --environment prd
      - python -m b3_platform.flow_specs.generate_databricks_bundle_root
      - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment dev
      - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment hml
      - python -m b3_platform.flow_specs.generate_databricks_resources_yaml --environment prd
      - databricks bundle validate -t dev
  build:
    commands:
      - echo "Use CodePipeline stages or manual approvals for hml/prd"
      - databricks bundle deploy -t dev
      - databricks bundle deploy -t hml
      - databricks bundle deploy -t prd
"""


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


def _provider_output_path(provider_name: str) -> Path:
    mapping = {
        "github_actions": Path("ci_adapters/github_actions/github-actions.yml"),
        "azure_devops": Path("ci_adapters/azure_devops/azure-pipelines.yml"),
        "bitbucket": Path("ci_adapters/bitbucket/bitbucket-pipelines.yml"),
        "aws": Path("ci_adapters/aws/buildspec.yml"),
    }
    return mapping[provider_name]


def _provider_template(provider_name: str) -> str:
    mapping = {
        "github_actions": GITHUB_ACTIONS_TEMPLATE,
        "azure_devops": AZURE_DEVOPS_TEMPLATE,
        "bitbucket": BITBUCKET_TEMPLATE,
        "aws": AWS_TEMPLATE,
    }
    return mapping[provider_name]


def _build_contract_markdown(provider_name: str, enabled: bool) -> str:
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


def _provider_contract_path(provider_name: str) -> Path:
    return Path(f"ci_adapters/{provider_name}/CONTRACT.md")


def generate_ci_adapters(write_all: bool = False) -> dict:
    providers = load_ci_providers()
    active_provider = get_active_ci_provider()

    generated_adapters = []
    generated_contracts = []
    disabled_providers = []

    for provider in providers:
        if not write_all and provider.name != active_provider.name:
            disabled_providers.append(provider.name)
            continue

        adapter_path = _provider_output_path(provider.name)
        adapter_path.parent.mkdir(parents=True, exist_ok=True)

        adapter_content = _provider_template(provider.name)
        if provider.name != active_provider.name:
            adapter_content = (
                "# PROVIDER DISABLED\n"
                "# Este adapter está gerado, porém não está ativo na configuração central.\n\n"
                + adapter_content
            )

        adapter_path.write_text(adapter_content, encoding="utf-8")
        generated_adapters.append(str(adapter_path))

        contract_path = _provider_contract_path(provider.name)
        contract_path.parent.mkdir(parents=True, exist_ok=True)
        contract_path.write_text(
            _build_contract_markdown(provider.name, provider.enabled),
            encoding="utf-8",
        )
        generated_contracts.append(str(contract_path))

    if write_all:
        disabled_providers = [
            provider.name for provider in providers if provider.name != active_provider.name
        ]

    return {
        "active_provider": active_provider.name,
        "disabled_providers": disabled_providers,
        "generated_adapters": generated_adapters,
        "generated_contracts": generated_contracts,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera adaptadores e contracts de CI/CD a partir da configuração central de providers."
    )
    parser.add_argument(
        "--write-all",
        default="false",
        choices=["true", "false"],
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    write_all = parsed.write_all.lower() == "true"
    result = generate_ci_adapters(write_all=write_all)

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
