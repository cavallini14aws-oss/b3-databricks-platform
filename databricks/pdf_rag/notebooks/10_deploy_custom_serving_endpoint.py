# Databricks notebook source
# MAGIC %pip install pyyaml requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.pdf_rag.src.deploy_custom_serving_endpoint import main

main()
