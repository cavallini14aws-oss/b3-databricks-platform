# bitbucket

- enabled: false

## Environments esperados
- development
- staging
- production

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
- dev: deploy controlado pela branch main
- hml: trigger manual recomendado
- prd: trigger manual + branch permissions + merge checks

## Fluxo recomendado
1. validate
2. deploy dev
3. deploy hml
4. deploy prd

## Observações
- Usar deployment environments do Bitbucket.
- Usar secured variables por deployment.
- PRD deve depender de merge controlado e permissão restrita.
