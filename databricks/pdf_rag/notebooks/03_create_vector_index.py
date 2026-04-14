# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Databricks notebook source
from src.vector_index_bootstrap import main

main()
