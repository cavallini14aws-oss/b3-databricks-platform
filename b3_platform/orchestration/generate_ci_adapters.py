import argparse
import json
from pathlib import Path

from b3_platform.orchestration.ci_provider_config import (
    get_active_ci_provider,
    load_ci_providers,
)


GITHUB_ACTIONS_TEMPLATE = """name: Databricks CI

on:
  pull_request:
  push:
    branches: [ "main" ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Compile Python
        run: python -m compileall b3_platform config pipelines resources sql

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


def generate_ci_adapters(write_all: bool = False) -> dict:
    providers = load_ci_providers()
    active_provider = get_active_ci_provider()

    written_files = []
    disabled_providers = []

    for provider in providers:
        if not write_all and provider.name != active_provider.name:
            disabled_providers.append(provider.name)
            continue

        output_path = _provider_output_path(provider.name)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        content = _provider_template(provider.name)
        if provider.name != active_provider.name:
            content = (
                "# PROVIDER DISABLED\n"
                "# Este adapter está gerado, porém não está ativo na configuração central.\n\n"
                + content
            )

        output_path.write_text(content, encoding="utf-8")
        written_files.append(str(output_path))

    return {
        "active_provider": active_provider.name,
        "disabled_providers": disabled_providers,
        "generated_files": written_files,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera adaptadores de CI/CD a partir da configuração central de providers."
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
