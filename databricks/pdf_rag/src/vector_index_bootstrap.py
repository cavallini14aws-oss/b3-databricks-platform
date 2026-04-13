from __future__ import annotations

import os

VECTOR_ENDPOINT_NAME = os.getenv("PDF_RAG_VECTOR_ENDPOINT_NAME", "pdf-rag-vs-endpoint")
VECTOR_INDEX_NAME = os.getenv("PDF_RAG_VECTOR_INDEX_NAME", "main.pdf_rag.pdf_chunks_index")
SOURCE_TABLE_NAME = os.getenv("PDF_RAG_SOURCE_TABLE_NAME", "main.pdf_rag.pdf_chunks")

print("[INFO] Vector Search bootstrap config")
print(f"[INFO] endpoint={VECTOR_ENDPOINT_NAME}")
print(f"[INFO] index={VECTOR_INDEX_NAME}")
print(f"[INFO] source_table={SOURCE_TABLE_NAME}")

print("[TODO] Create endpoint and index using Databricks SDK or REST API")
print("[TODO] Point index to source Delta table with embedding column")
