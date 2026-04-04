# github_actions

- enabled: true

## Environments esperados
- dev
- hml
- prd

## Secrets obrigatórios por ambiente
### dev
- DEV_WORKSPACE_HOST
- DEV_DATABRICKS_TOKEN
- DEV_CLUSTER_ID

### hml
- HML_WORKSPACE_HOST
- HML_DATABRICKS_TOKEN
- HML_CLUSTER_ID

### prd
- PRD_WORKSPACE_HOST
- PRD_DATABRICKS_TOKEN
- PRD_CLUSTER_ID

## Modelo de aprovação recomendado
- dev: sem required reviewers
- hml: reviewer opcional ou gate simples
- prd: required reviewers obrigatório e impedir self-review

## Fluxo recomendado
1. validate
2. deploy dev
3. deploy hml
4. deploy prd

## Observações
- Usar GitHub Environments para dev, hml e prd.
- Segregar secrets por environment.
- PRD deve usar credencial isolada de produção.
