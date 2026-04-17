# HANDOFF CIRÚRGICO DO PDF RAG V1

## 1. Objetivo

Este documento congela o estado real da V1 do PDF RAG Databricks-first.

A V1 é a referência funcional e operacional do fluxo RAG sobre PDFs em Databricks, validada em ambiente dev/free edition.

Ela não deve ser confundida com a futura V2 MLflow.

## 2. Estado atual

A V1 já possui uma esteira funcional com:

- PDFs em Unity Catalog Volume
- inspeção documental
- ingestão incremental
- auto-discovery de PDFs
- chunking persistido em Delta
- Change Data Feed habilitado
- Vector Search criado e sincronizado
- query vetorial validada
- resposta RAG em português brasileiro
- cascata de Foundation Models por ambiente
- smoke E2E via terminal

## 3. Componentes principais

Notebooks principais:

- databricks/pdf_rag/notebooks/01_inspect_documents.py
- databricks/pdf_rag/notebooks/02_ingest_pages_and_chunks.py
- databricks/pdf_rag/notebooks/03_create_vector_index.py
- databricks/pdf_rag/notebooks/04_query_vector_index.py
- databricks/pdf_rag/notebooks/05_rag_answer_ptbr.py

Scripts operacionais principais:

- bin/dbx-submit-pdf-rag-vector-refresh.py
- bin/dbx-submit-pdf-rag-vector-query-smoke.py
- bin/dbx-submit-pdf-rag-answer-smoke.py
- bin/dbx-run-pdf-rag-e2e-smoke.py

## 4. Storage documental

Volume usado para os PDFs:

dbfs:/Volumes/workspace/pdf_rag/raw_docs/

A V1 suporta auto-discovery de PDFs nesse volume.

## 5. Tabelas Delta

As tabelas documentais operam em:

workspace.pdf_rag

Tabelas principais:

- pdf_documents
- pdf_pages
- pdf_chunks

A tabela pdf_chunks é a base para o Vector Search.

## 6. Vector Search

A V1 já validou:

- criação do índice
- habilitação de CDF
- refresh/sync do índice
- query vetorial
- retry de readiness antes do sync

## 7. Foundation Models

A V1 usa configuração por ambiente em:

databricks/pdf_rag/config/pdf_rag_foundation_models.yml

Em dev/free edition, a estratégia correta é:

endpoint_strategy: first_available

Em produção, a estratégia correta é:

endpoint_strategy: strict

## 8. Comportamento real no Free Edition

Foi validado que endpoints databricks-gpt-5-* podem aparecer como disponíveis, mas falhar por rate limit 0 no Free Edition.

Isso não é bug da V1.

A V1 deve continuar usando cascata em dev.

## 9. Regra de execução em dev

Para execução normal em dev:

export LOCAL_ENV="dev"
unset DATABRICKS_FOUNDATION_ENDPOINT

Isso permite que a cascata selecione o primeiro endpoint funcional.

## 10. Override explícito

Se DATABRICKS_FOUNDATION_ENDPOINT estiver definido, o sistema respeita o endpoint forçado e não usa cascata.

Esse comportamento é intencional.

## 11. Runner E2E

Runner E2E:

./bin/dbx-run-pdf-rag-e2e-smoke.py

Ele executa:

1. import dos Workspace Files
2. refresh do Vector Search
3. query vetorial
4. resposta RAG PT-BR
5. geração de evidência local

## 12. Evidências runtime

As evidências locais ficam em:

artifacts/pdf_rag/

Essas evidências não devem ser versionadas.

O correto é versionar o runner, não a evidência.

## 13. O que não deve ser alterado agora

Não refatorar a V1 para MLflow.

Não misturar V1 e V2.

Não tentar corrigir endpoint GPT bloqueado no Free Edition como bug de código.

Não commitar arquivos locais de runtime, input, secrets ou artifacts.

## 14. Papel da V1

A V1 é:

- blueprint funcional
- base de estudo
- base de debug
- base de demonstração
- referência operacional

A V1 deve permanecer intacta quando a V2 for criada.
