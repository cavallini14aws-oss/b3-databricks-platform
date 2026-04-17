# PROMOTION PATH OFICIAL DO PDF RAG

## 1. Objetivo

Este documento define a estrada de promoção do PDF RAG entre:

dev/free edition -> hml oficial -> prd oficial

A intenção é separar claramente o comportamento flexível de desenvolvimento do comportamento governado esperado em ambientes oficiais.

## 2. Princípio central

O Free Edition serve para validar arquitetura, fluxo técnico e comportamento funcional.

Ambientes oficiais devem validar governança, identidade, quota, permissões, endpoint aprovado, observabilidade e previsibilidade operacional.

## 3. DEV / Free Edition

### Objetivo

Validar a funcionalidade da esteira.

### Estratégia

endpoint_strategy: first_available

### Motivo

No Free Edition, alguns endpoints podem estar visíveis, mas indisponíveis por quota ou rate limit.

Por isso, dev deve tolerar fallback.

### Regra operacional

export LOCAL_ENV="dev"
unset DATABRICKS_FOUNDATION_ENDPOINT

### Critérios mínimos de sucesso

- ingestão incremental funciona
- chunks são persistidos
- CDF está habilitado
- Vector Search sincroniza
- query vetorial retorna contexto
- RAG PT-BR responde
- smoke E2E passa

## 4. HML oficial

### Objetivo

Validar comportamento controlado antes de produção.

### Estratégia recomendada

endpoint_strategy: first_available

Mas com lista menor e aprovada de endpoints.

### Critérios mínimos

- workspace oficial configurado
- permissões validadas
- identidade de execução definida
- endpoint de Foundation Model aprovado
- quotas validadas
- Vector Search validado
- jobs oficiais configurados
- logs e evidências revisáveis

## 5. PRD oficial

### Objetivo

Executar com previsibilidade e governança.

### Estratégia obrigatória

endpoint_strategy: strict

Produção não deve depender de heurística de fallback sem aprovação.

O endpoint produtivo precisa ser conhecido, aprovado e monitorado.

### Estado atual

default_endpoint_name: TO_BE_DEFINED

Isso é intencional até existir endpoint corporativo oficial.

## 6. Critérios para promover de DEV para HML

Antes de sair de dev/free edition, validar:

- configuração de workspace oficial
- catálogo/schema oficiais
- volume oficial de PDFs
- endpoint oficial de Foundation Model
- permissões de leitura e escrita
- permissões de Vector Search
- jobs oficiais
- logs operacionais
- smoke E2E em HML

## 7. Critérios para promover de HML para PRD

Antes de produção, validar:

- endpoint produtivo definido
- estratégia strict ativa
- quotas confirmadas
- identidade oficial validada
- permissões mínimas aplicadas
- rollback definido
- runbook operacional criado
- smoke produtivo controlado
- evidência de execução aprovada

## 8. Diferença entre first_available e strict

### first_available

Usado para desenvolvimento e validação flexível.

Tenta endpoints em ordem até encontrar um funcional.

### strict

Usado para produção.

Usa apenas o endpoint definido.

Se o endpoint falhar, a execução falha.

## 9. Regra para override

Se DATABRICKS_FOUNDATION_ENDPOINT estiver definido, ele força o endpoint.

Nesse caso, a cascata não deve ser usada.

## 10. Arquivos locais que não devem ser promovidos

Nunca promover:

- .state/
- pdf_rag_input/
- artifacts/
- .env.databricks.*.local
- .secrets.databricks.*.local
- .secrets.databricks.*.oauth.local

## 11. Resultado esperado

O promotion path correto é:

dev flexível para validar funcionamento
hml controlado para validar operação oficial
prd strict para execução governada
