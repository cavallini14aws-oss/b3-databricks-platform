# Databricks notebook source
# MAGIC %pip install pyyaml requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from src.deploy_custom_serving_endpoint import main

main()
