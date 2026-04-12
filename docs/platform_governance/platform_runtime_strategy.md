# Platform Runtime Strategy

## Regra
A plataforma deve fornecer um runtime oficial fechado para:
- pipeline convencional
- ML
- MLOps
- IA / RAG

## Arquivos
- requirements-platform.txt
- requirements-rag-lab.txt
- requirements-dev.txt

## Ambientes
### DEV
Pode usar pip install manual para experimento.

### HML
Deve usar runtime oficial. Exceção somente com aprovação versionada.

### PRD
Deve usar runtime oficial. Exceção somente com aprovação versionada.

## Objetivo
Evitar drift de dependência entre dev, hml e prd.
