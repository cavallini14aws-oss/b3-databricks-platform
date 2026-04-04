# CI/CD adapters

## Objetivo
Fornecer templates de esteira para clientes com diferentes ferramentas de CI/CD,
sem acoplar a plataforma a uma única solução.

## Disponíveis
- azure_devops/azure-pipelines.yml
- bitbucket/bitbucket-pipelines.yml

## Princípios
- desenvolvedor não faz deploy direto em PRD
- PRD exige aprovação manual
- cada ambiente usa credencial/profile próprio
- a plataforma gera os artefatos neutros
- a esteira do cliente consome os artefatos gerados

## Ajustes esperados no cliente
- trocar `echo "databricks bundle ..."` pelos comandos reais
- configurar secrets/variables:
  - DEV_WORKSPACE_HOST
  - HML_WORKSPACE_HOST
  - PRD_WORKSPACE_HOST
  - DEV_PROFILE / HML_PROFILE / PRD_PROFILE
  - DEV_CLUSTER_ID / HML_CLUSTER_ID / PRD_CLUSTER_ID
- configurar approvals/gates na ferramenta escolhida

## Governança recomendada
- dev: automático
- hml: approval opcional
- prd: approval obrigatório + service principal de produção
