# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
from pathlib import Path

import yaml
from databricks.vector_search.client import VectorSearchClient

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

dbutils.widgets.text(
    "query_text",
    "Explique o significado psicológico do Livro Vermelho de Jung.",
)
query_text = dbutils.widgets.get("query_text")

cfg = yaml.safe_load(
    Path(f"{PROJECT_ROOT}/config/pdf_rag_vector_search.yml").read_text(encoding="utf-8")
)["vector_search"]

client = VectorSearchClient()
index = client.get_index(
    endpoint_name=cfg["endpoint_name"],
    index_name=cfg["index_name"],
)

results = index.similarity_search(
    query_text=query_text,
    columns=cfg["query_columns"],
    num_results=5,
)

print("[OK] Vector Search query executed")
print(f"[INFO] query_text={query_text}")
print(results)
