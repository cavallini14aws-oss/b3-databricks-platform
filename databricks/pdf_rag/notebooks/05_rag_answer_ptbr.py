# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch pyyaml requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

dbutils.widgets.text(
    "question",
    "Explique o significado psicológico do Livro Vermelho de Jung.",
)

dbutils.widgets.text(
    "foundation_endpoint",
    "databricks-gpt-5-4-mini",
)

question = dbutils.widgets.get("question")
foundation_endpoint = dbutils.widgets.get("foundation_endpoint")

os.environ["DATABRICKS_FOUNDATION_ENDPOINT"] = foundation_endpoint

from src.rag_answer_ptbr import main

main(question)
