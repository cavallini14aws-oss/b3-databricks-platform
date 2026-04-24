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
- DEV_DEPLOY_USER

### hml
- HML_WORKSPACE_HOST
- HML_DATABRICKS_TOKEN
- HML_DEPLOY_USER

### prd
- PRD_WORKSPACE_HOST
- PRD_DATABRICKS_TOKEN
- PRD_DEPLOY_USER

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
