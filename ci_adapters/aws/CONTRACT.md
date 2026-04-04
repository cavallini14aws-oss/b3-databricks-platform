# aws

- enabled: false

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
- dev: deploy automático ou via stage inicial
- hml: approval opcional em stage intermediário
- prd: approval obrigatório no CodePipeline antes do deploy

## Fluxo recomendado
1. validate
2. deploy dev
3. deploy hml
4. deploy prd

## Observações
- Usar CodePipeline/CodeBuild como adaptador.
- Usar Secrets Manager ou Parameter Store.
- PRD deve usar credencial isolada de produção.
