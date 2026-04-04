# azure_devops

- enabled: false

## Environments esperados
- databricks-dev
- databricks-hml
- databricks-prd

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
- dev: deploy automático ou semi-automático
- hml: approval opcional conforme cliente
- prd: Approvals and checks obrigatório no environment de PRD

## Fluxo recomendado
1. validate
2. deploy dev
3. deploy hml
4. deploy prd

## Observações
- Usar Environments no Azure DevOps.
- Usar variable groups ou secret variables segregadas por ambiente.
- PRD deve usar credencial isolada de produção.
