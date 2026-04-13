# PDF RAG Lab - Databricks Native Mode

This pilot targets Databricks-native inference.

Ollama was used only for local fallback validation.

Main local secure flow:

1. Copy `.env.databricks.example` to one of:
   - `.env.databricks.dev.local`
   - `.env.databricks.hml.local`
   - `.env.databricks.prd.local`

2. Fill:
   - DATABRICKS_HOST
   - DATABRICKS_TOKEN
   - DATABRICKS_SERVING_ENDPOINT

3. Load environment:
   source bin/use-databricks-official dev my-endpoint

4. Validate:
   ./bin/dbx-rag-smoke
