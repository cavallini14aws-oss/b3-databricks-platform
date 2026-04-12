# PDF RAG Lab - Operations

## Objective

The PDF RAG Lab is the governed example of a PDF-based Retrieval-Augmented Generation workflow inside the B3 Databricks Platform.

Its target architecture is Databricks-native inference.

Ollama is used only as a temporary local fallback for development and validation in the free environment.

## Provider strategy

- Local validation fallback: `ollama`
- Official target provider: `databricks`

The final intended architecture is based on Databricks-native capabilities such as Model Serving or Foundation Model API.

## Current validated local state

The following local flow has already been validated:

- PDF files loaded into `pipelines/examples/pdf_rag_lab/backend/data/raw`
- `setup_docs.py` executed successfully
- local vector index generated successfully
- FastAPI backend started successfully
- `/health` endpoint returning `ok`
- `/chat` endpoint returning contextual answers with retrieved chunks
- automated test suite passing

Validated evidence so far:
- 181 tests passed
- 1 warning
- dual provider path implemented
- local default provider set to `ollama`

## Relevant paths

- Backend entrypoint:
  - `pipelines/examples/pdf_rag_lab/backend/main.py`

- Services:
  - `pipelines/examples/pdf_rag_lab/backend/services/config.py`
  - `pipelines/examples/pdf_rag_lab/backend/services/pdf_service.py`
  - `pipelines/examples/pdf_rag_lab/backend/services/embedding_service.py`
  - `pipelines/examples/pdf_rag_lab/backend/services/vector_store.py`
  - `pipelines/examples/pdf_rag_lab/backend/services/llm_service.py`

- Setup script:
  - `pipelines/examples/pdf_rag_lab/backend/setup_docs.py`

- Raw documents:
  - `pipelines/examples/pdf_rag_lab/backend/data/raw`

## Local prerequisites

Expected local prerequisites:

- Python virtual environment activated
- project dependencies installed
- Ollama available locally only when using `LLM_PROVIDER=ollama`
- local model available only for local fallback usage

## Local provider configuration

Current local defaults in `config.py`:

- `LLM_PROVIDER = ollama`
- `DEFAULT_LLM_MODEL = llama3.1:8b`

These defaults are intentional for local development in the free environment.

They do not represent the final official target architecture.

## How to index documents locally

Place PDF files in:

`pipelines/examples/pdf_rag_lab/backend/data/raw`

Then run:

`python pipelines/examples/pdf_rag_lab/backend/setup_docs.py`

## How to start the backend locally

`./bin/run-pdf-rag-lab-local`

## Health check

`curl http://127.0.0.1:8010/health`

Expected result:

`{"status":"ok"}`

## Chat test

`./bin/test-pdf-rag-lab-api`

## Automated tests

`python -m pytest -q`

Latest validated result:
- 181 passed
- 1 warning

## Databricks-native target mode

The lab is already prepared to support Databricks as LLM provider.

Relevant environment variables:

- `LLM_PROVIDER=databricks`
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_SERVING_ENDPOINT`

Official target path:

1. Provision secrets and secure runtime variables
2. Define the real serving endpoint
3. Configure catalog, schema, volume and grants
4. Switch `LLM_PROVIDER` to `databricks`
5. Repeat the same validated flow:
   - setup docs
   - start backend
   - run health check
   - run chat test

## Known current limitations

Current lab limitations:

- no conversational memory
- no advanced reranker
- no query rewriting
- no frontend
- retrieval still needs maturity for broader corpora
- Databricks provider not yet validated end-to-end in a real official workspace

## Operational recommendation

For serious local validation, use a controlled corpus with one PDF at a time.

Reason:
larger mixed corpora degrade retrieval quality in the current stage and make debugging less precise.

## Troubleshooting

### Issue: Databricks provider fails

Likely cause:
- `DATABRICKS_HOST` not configured
- missing token
- missing endpoint

### Issue: local startup reports address already in use

Likely cause:
- the API is already running on port `8010`

Check with:

`lsof -i :8010`

### Issue: weak answer quality

Likely cause:
- noisy corpus
- retrieval returning distant chunks
- prompt too generic

### Issue: local LLM not responding

Likely cause:
- Ollama not running
- local model not pulled
- incorrect local provider config

## Current architectural status

This lab is no longer a disconnected proof of concept.

It is currently:
- governed
- reproducible
- testable
- integrated into the platform structure
- locally validated
- prepared for Databricks-native evolution

The next milestone is to validate the Databricks provider end-to-end in the official environment without losing the governance and reproducibility already established locally.
