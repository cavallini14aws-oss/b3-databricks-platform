# Dependencias devem vir de ambiente controlado/artifact versionado.
# Instalacao manual via pip foi removida deste notebook para uso em official.

# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Databricks notebook source
from databricks.vector_search.client import VectorSearchClient
import yaml
from pathlib import Path

cfg = yaml.safe_load(
    Path("databricks/pdf_rag/config/pdf_rag_vector_search.yml").read_text(encoding="utf-8")
)["vector_search"]

query_text = "Quais documentos falam sobre sonhos e inconsciente?"

client = VectorSearchClient()
index = client.get_index(
    endpoint_name=cfg["endpoint_name"],
    index_name=cfg["index_name"],
)

results = index.similarity_search(
    query_text=query_text,
    columns=cfg["query_columns"],
    num_results=5,
    query_type="hybrid",
)

display(results)
