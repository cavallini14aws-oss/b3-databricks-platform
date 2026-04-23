# OFFICIAL INTEGRATION RUNBOOK

## Objetivo
Este runbook define a sequência operacional mínima para promover o projeto ao ambiente Databricks official sem improviso, sem drift e sem promoção de artefatos locais indevidos.

## Escopo
Este documento cobre:

- dry-run oficial
- deploy oficial
- smoke pós-deploy
- rollback declarativo e reapply/smoke opcional
- critérios mínimos de readiness
- empacotamento clean de release

## Pré-requisitos obrigatórios antes de qualquer deploy official

### Identidade
Preencher no contrato official e nos ambientes CI/CD:

- workspace host real por ambiente
- credenciais reais por ambiente
- service principals reais para hml/prd
- grupos reais
- secret scopes reais
- cluster policies reais

### Contrato official
Arquivo:

- `config/official_environment_contract.yml`

Campos que não podem permanecer indefinidos antes de official real:

- `service_principals`
- `groups`
- `secret_scopes`
- `cluster_policies`

Campos que podem existir como placeholder durante preparação estrutural:

- nomes temporários de volumes
- nomes temporários de catálogos e schemas, enquanto ainda não houver definição final institucional

## Política de ambientes

### dev
- pode usar current user
- pode usar root path em `/Workspace/Users/...`
- foco em validação rápida

### hml
- deve operar com postura de produção
- deve usar root path compartilhado
- deve preferir service principal

### prd
- deve operar com postura de produção
- deve preferir service principal
- não promover sem dry-run, smoke e readiness explícitos

## Sequência oficial recomendada

### 1. Validar working tree
Garantir branch limpa e sem arquivos locais relevantes fora do Git.

### 2. Validar contrato official
Executar:

- `./bin/check-official-readiness-contract dev soft`
- `./bin/check-official-readiness-contract hml soft`
- `./bin/check-official-readiness-contract prd soft`

Quando houver dados reais de produção preenchidos, executar também:

- `./bin/check-official-readiness-contract hml strict`
- `./bin/check-official-readiness-contract prd strict`

### 3. Validar empacotamento clean
Executar:

- `./bin/check-official-release-clean`

### 4. Gerar release clean
Executar:

- `./bin/package-official-release-clean`

### 4. Dry-run official
Executar:

- `./bin/dry-run-official-deploy dev`
- `./bin/dry-run-official-deploy hml`
- `./bin/dry-run-official-deploy prd`

Observação:
- o dry-run pode operar em modo declarativo quando ainda não houver auth official
- quando houver auth real, ativar bundle validate/plan/summary remoto

### 5. Deploy official
Executar o adapter oficial correspondente ao ambiente alvo.

### 6. Smoke pós-deploy
Executar:

- `./bin/smoke-official-release-adapter <env>`

### 7. Rollback, se necessário
Executar:

- `./bin/rollback-official-release <env> <snapshot_name>`

Observação:
- o rollback atual restaura estado declarativo
- reapply e smoke pós-rollback devem ser executados quando disponíveis no ambiente official real

## Critérios mínimos para promover hml/prd

- contrato official preenchido para o ambiente
- dry-run sem erro
- artefato clean gerado
- smoke do ambiente anterior executado com sucesso
- sem drift entre `config/` e `data_platform/resources/config/`
- suite de testes verde
- sem arquivos locais ou temporários incluídos no release

## Itens proibidos no pacote official

Não podem entrar no release:

- `.git/`
- `.databricks/bundle/`
- `.state/`
- `.local_backup/`
- `.tmp_env_audit/`
- `build/`
- `dist/`
- `__MACOSX/`
- `__pycache__/`
- `.pytest_cache/`
- `*.egg-info/`

## LLM RAG standard e MLflow

Antes de official real, validar explicitamente:

- entrypoints versionados presentes
- registry consistente
- smoke funcional do fluxo standard
- smoke funcional do fluxo mlflow
- variáveis e endpoints alinhados ao ambiente real

## Checklist final antes de official real

- [ ] contrato official preenchido
- [ ] release clean gerado
- [ ] dry-run executado
- [ ] deploy planejado
- [ ] smoke definido
- [ ] rollback definido
- [ ] sem drift de config
- [ ] sem sujeira local no pacote
- [ ] suite verde
