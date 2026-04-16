# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.vector_index_bootstrap import main


# Vector Search Delta Sync requires Change Data Feed on the source table.
spark.sql("""
ALTER TABLE workspace.pdf_rag.pdf_chunks
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
print("[OK] Change Data Feed enabled for workspace.pdf_rag.pdf_chunks")

main()
