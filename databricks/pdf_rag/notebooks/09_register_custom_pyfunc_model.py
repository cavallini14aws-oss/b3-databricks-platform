# Dependencias devem vir de ambiente controlado/artifact versionado.
# Instalacao manual via pip foi removida deste notebook para uso em official.

# Databricks notebook source
# MAGIC %pip install mlflow pyyaml pandas requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.pdf_rag.src.register_custom_pyfunc_model import main

main()
