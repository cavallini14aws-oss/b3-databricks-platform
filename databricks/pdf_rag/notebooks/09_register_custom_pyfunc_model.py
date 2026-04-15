# Databricks notebook source
# MAGIC %pip install mlflow pyyaml pandas requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from src.register_custom_pyfunc_model import main

main()
