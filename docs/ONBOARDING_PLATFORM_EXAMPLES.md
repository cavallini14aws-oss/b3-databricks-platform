# Onboarding Executivo — Exemplos Oficiais da Plataforma

Este documento orienta os desenvolvedores sobre qual exemplo oficial usar como base para cada tipo de demanda dentro da plataforma Databricks.

## Regra de ouro
O desenvolvedor deve partir de um exemplo oficial e adaptar a lógica de negócio.
O desenvolvedor não deve reinventar arquitetura, governança, tagging, masking ou fluxo de workload por conta própria.

---

## 1. Demandas de Dados

### 1.1 Ingestão bruta / entrada de arquivos / carga inicial
Use:
- `examples/databricks/example_data_bronze`

Quando usar:
- recepção de arquivo
- ingestão de tabela legado
- recepção inicial de dado bruto
- captura de dado sem transformação forte

O que o dev altera:
- origem
- lista de colunas
- regra de leitura
- nome da tabela
- domínio de negócio

O que o dev não altera sem necessidade:
- padrão de governança
- tagging
- campos técnicos
- política de masking

---

### 1.2 Higienização / padronização / enriquecimento
Use:
- `examples/databricks/example_data_silver`

Quando usar:
- limpeza de dados
- normalização
- padronização textual
- enriquecimento
- unificação antes de consumo

O que o dev altera:
- regras de transformação
- joins/enriquecimentos
- colunas de negócio
- validações específicas

O que o dev não altera sem necessidade:
- estrutura de governança
- convenção de tags
- decisão explícita de masking

---

### 1.3 Camada consolidada / consumo / regra final de negócio
Use:
- `examples/databricks/example_data_gold`

Quando usar:
- agregação final
- visão para consumo analítico
- tabela pronta para dashboard/produto
- output consolidado do domínio

O que o dev altera:
- regra de agregação
- granularidade
- métricas finais
- chave de consumo

O que o dev não altera sem necessidade:
- governança
- descrição de colunas
- convenção de tags

---

## 2. Demandas de RAG

### 2.1 RAG padrão
Use:
- `examples/databricks/example_rag_standard`

Quando usar:
- chunking
- recuperação simples
- embeddings
- RAG sem trilha MLflow
- experimentação controlada sem registry/serving formal

Workload sugerido:
- `type: rag`
- `variant: standard`

O que o dev altera:
- origem documental
- chunking
- embeddings
- lógica de retrieval
- estratégia de resposta

O que o dev não altera sem necessidade:
- governança da tabela
- tagging
- política de masking para texto sensível
- convenção de workload

---

### 2.2 RAG com trilha MLflow
Use:
- `examples/databricks/example_rag_mlflow`

Quando usar:
- necessidade de tracking
- integração com registry
- serving controlado
- evolução de RAG para trilha mais formal

Workload sugerido:
- `type: rag`
- `variant: mlflow`

O que o dev altera:
- pipeline documental
- estratégia de retrieval
- integração real com MLflow
- serving/model path

O que o dev não altera sem necessidade:
- variant
- governança
- tagging
- política de masking do conteúdo

---

## 3. Demandas de Machine Learning

### 3.1 Treino
Use:
- `examples/databricks/example_ml_training`

Quando usar:
- preparação de dataset
- treino
- avaliação
- tracking técnico
- preparação de artefato/modelo

Workload sugerido:
- `type: ml`

O que o dev altera:
- dataset
- features
- algoritmo
- métrica
- estratégia de treino

O que o dev não altera sem necessidade:
- governança da tabela técnica
- tagging
- política de masking quando houver PII em dataset

---

### 3.2 Inferência / scoring
Use:
- `examples/databricks/example_ml_inference`

Quando usar:
- scoring batch
- inferência operacional
- saída de predição
- categorização de score

Workload sugerido:
- `type: ml`

O que o dev altera:
- origem das features
- carregamento real do modelo
- regra de predição
- formatação da saída

O que o dev não altera sem necessidade:
- governança
- tagging
- convenções técnicas do output

---

## 4. Regras obrigatórias da plataforma

Toda nova tabela deve nascer com:
- descrição da tabela
- descrição campo a campo
- tags de tabela
- tags coluna a coluna
- classificação de campo
- decisão explícita sobre masking/anonimização quando houver PII

Nenhuma implementação nova deve:
- subir sem governança
- criar tabela sem `COMMENT`
- criar tabela sem `SET TAGS`
- omitir `pii`, `classificacao` e `mascaramento`
- ignorar masking quando houver sensibilidade
- inventar fluxo fora do padrão de workload

---

## 5. Resumo rápido de escolha

Se a demanda é:
- ingestão bruta → `example_data_bronze`
- limpeza / padronização → `example_data_silver`
- consumo consolidado → `example_data_gold`
- RAG simples → `example_rag_standard`
- RAG com tracking/registry/serving → `example_rag_mlflow`
- treino de modelo → `example_ml_training`
- inferência de modelo → `example_ml_inference`
