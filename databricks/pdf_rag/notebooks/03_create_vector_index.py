# Dependencias devem vir de ambiente controlado/artifact versionado.
# Instalacao manual via pip foi removida deste notebook para uso em official.

# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Databricks notebook source
from databricks.pdf_rag.src.vector_index_bootstrap import main

main()
