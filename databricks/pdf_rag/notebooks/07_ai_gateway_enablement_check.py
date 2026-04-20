# Dependencias devem vir de ambiente controlado/artifact versionado.
# Instalacao manual via pip foi removida deste notebook para uso em official.

# Databricks notebook source
# MAGIC %pip install pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pathlib import Path
import yaml
import os

cfg = yaml.safe_load(
    Path("databricks/pdf_rag/config/pdf_rag_ai_gateway.yml").read_text(encoding="utf-8")
)["ai_gateway"]

endpoint_name = os.getenv(cfg["endpoint_name_env"])

print("[INFO] AI Gateway enablement checklist")
print(f"[INFO] endpoint env var name = {cfg['endpoint_name_env']}")
print(f"[INFO] endpoint resolved = {endpoint_name}")
print(f"[INFO] inference tables target = {cfg['inference_table_catalog']}.{cfg['inference_table_schema']}")
print(f"[INFO] inference table prefix = {cfg['inference_table_prefix']}")
print()
print("[TODO] In the Databricks UI:")
print("1. Open the serving endpoint / foundation endpoint")
print("2. Edit AI Gateway")
print("3. Enable inference tables")
print("4. Choose catalog/schema")
print("5. Save and wait until endpoint is ready")
