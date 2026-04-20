# Biblioteca oficial de exemplos Databricks

Esta pasta contém exemplos oficiais da plataforma para servir como base de implementação dos desenvolvedores.

## Princípios obrigatórios
Toda nova tabela deve nascer com:
- descrição da tabela
- descrição campo a campo
- tags de tabela
- tags coluna a coluna
- classificação de campos
- decisão explícita sobre masking/anonimização quando houver PII

## Exemplos disponíveis
- example_data_bronze
- example_data_silver
- example_data_gold
- example_rag_standard
- example_rag_mlflow
- example_ml_training
- example_ml_inference

## Escolha rápida

### Dados
- ingestão bruta → `example_data_bronze`
- limpeza / padronização → `example_data_silver`
- consumo consolidado → `example_data_gold`

### RAG
- sem trilha MLflow → `example_rag_standard`
- com tracking/registry/serving → `example_rag_mlflow`

### ML
- treino → `example_ml_training`
- inferência / scoring → `example_ml_inference`

## Leitura obrigatória
Antes de iniciar uma implementação nova, consultar:
- `docs/ONBOARDING_PLATFORM_EXAMPLES.md`

## Padrão esperado
- pipeline de referência
- DDL de tabela
- aplicação de tags
- política de masking quando aplicável
