# Databricks notebook source
# MAGIC %pip install pyyaml requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.pdf_rag.src.smoke_custom_serving import main

main()
