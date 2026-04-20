# Dependencias devem vir de ambiente controlado/artifact versionado.
# Instalacao manual via pip foi removida deste notebook para uso em official.

# Databricks notebook source
# MAGIC %pip install pyyaml requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.pdf_rag.src.smoke_foundation_model import main

main()
