# Databricks notebook source
# MAGIC %pip install pyyaml requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from src.smoke_foundation_model import main

main()
