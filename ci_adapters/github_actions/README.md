# GitHub Actions adapter

## Objetivo
Executar validate e deploy do bundle Databricks com promoção controlada entre:
- dev
- hml
- prd

## Arquivo principal
- ci_adapters/github_actions/github-actions.yml

## Environments obrigatórios no GitHub
Criar os seguintes environments no repositório:
- dev
- hml
- prd

## Regras recomendadas
### dev
- sem required reviewers
- deploy pode ocorrer automaticamente após merge em main

### hml
- reviewer opcional, conforme maturidade do cliente
- recomendado usar ao menos um gate simples

### prd
- required reviewers obrigatório
- impedir self-review
- secrets de produção visíveis apenas no environment prd

## Secrets obrigatórios
### Environment dev
- DEV_WORKSPACE_HOST
- DEV_DATABRICKS_TOKEN
- DEV_CLUSTER_ID

### Environment hml
- HML_WORKSPACE_HOST
- HML_DATABRICKS_TOKEN
- HML_CLUSTER_ID

### Environment prd
- PRD_WORKSPACE_HOST
- PRD_DATABRICKS_TOKEN
- PRD_CLUSTER_ID

## Validação antecipada
O workflow executa:
- `python -m data_platform.orchestration.validate_active_ci_provider`

Esse comando valida o contrato central de secrets do provider ativo antes do validate/deploy do bundle.

## Fluxo de execução
1. pull request roda validate
2. validate checa contrato de secrets do provider ativo
3. merge em main libera deploy dev
4. após dev, workflow segue para hml
5. após hml, workflow segue para prd
6. prd só executa se o environment prd liberar

## Governança recomendada
- desenvolvedor não deve aprovar prd
- prd deve ser aprovado por reviewer, arquiteto ou release manager
- token de prd deve pertencer a credencial isolada de produção

## Observação
O core da plataforma continua neutro.
Este adapter é apenas a camada GitHub Actions.
