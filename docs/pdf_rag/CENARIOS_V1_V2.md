# CENÁRIOS V1 E V2 DO PDF RAG

## Visão geral

O projeto passa a manter duas versões complementares.

## Cenário 1 — V1 Basic

### O que é
A versão funcional e operacional do PDF RAG.

### Objetivo
Servir como blueprint funcional do fluxo:
- ingestão
- chunking
- vector search
- resposta PT-BR
- smoke E2E

### Quando usar
- onboarding
- entendimento da arquitetura
- debug
- ajustes de retrieval e chunking
- referência funcional

### Para quem serve
- data engineers
- engenheiros de plataforma
- desenvolvedores que precisam entender a base operacional

## Cenário 2 — V2 MLflow

### O que é
A versão apartada que reaproveita a ideia e a lógica central da V1, mas adiciona:
- empacotamento como modelo
- registro
- serving
- governança de inferência

### Objetivo
Servir como blueprint de profissionalização da camada de inferência.

### Quando usar
- registro de modelo
- pyfunc
- serving endpoint
- validação de deploy
- preparação para ambientes oficiais

### Para quem serve
- engenheiros de ML/MLOps
- arquitetos
- desenvolvedores que precisam transformar a lógica em ativo servível

## Relação entre as duas

A V1 não substitui a V2.
A V2 não invalida a V1.

As duas são complementares:
- V1 ensina como funciona
- V2 ensina como profissionalizar

## Regra de ouro

- V1 = referência funcional
- V2 = referência MLflow/serving
