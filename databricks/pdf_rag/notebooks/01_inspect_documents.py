# Databricks notebook source
# MAGIC %pip install pymupdf pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime
from pathlib import Path
import json
import os
import sys
import yaml

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.document_catalog import load_document_catalog
from src.document_intelligence import inspect_document
from src.document_quality_report import write_document_quality_report

tables_cfg_path = Path(PROJECT_ROOT) / "config" / "pdf_rag_tables.yml"
with open(tables_cfg_path, "r", encoding="utf-8") as f:
    tables_cfg = yaml.safe_load(f)

documents_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['documents']}"
reports_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['quality_reports']}"

catalog = load_document_catalog("/Volumes/workspace/pdf_rag/raw_docs")

existing_files = set()
if spark.catalog.tableExists(documents_table):
    existing_files = {
        row["file_name"]
        for row in spark.table(documents_table).select("file_name").distinct().collect()
    }

catalog = [
    doc for doc in catalog
    if str(doc["source_file_name"]) not in existing_files
]

print(f"[INFO] new_documents_to_ingest={len(catalog)}")

rows_documents = []
rows_reports = []

for doc in catalog:
    source_file_name = doc["source_file_name"]
    pdf_path = f"/Volumes/workspace/pdf_rag/raw_docs/{source_file_name}"

    inspection = inspect_document(pdf_path, source_file_name)
    report_path = write_document_quality_report(inspection)
    now_ts = datetime.utcnow()

    rows_documents.append(
        (
            str(doc["document_id"]),
            str(source_file_name),
            str(doc["display_name"]),
            str(inspection.quality_tier),
            str(inspection.extraction_strategy),
            int(inspection.usable_pages),
            int(inspection.total_pages),
            float(inspection.avg_ocr_noise_score),
            now_ts,
        )
    )

    rows_reports.append(
        (
            str(doc["document_id"]),
            str(source_file_name),
            json.dumps(
                {
                    "file_name": inspection.file_name,
                    "total_pages": int(inspection.total_pages),
                    "usable_pages": int(inspection.usable_pages),
                    "unusable_pages": int(inspection.unusable_pages),
                    "avg_ocr_noise_score": float(inspection.avg_ocr_noise_score),
                    "quality_tier": inspection.quality_tier,
                    "extraction_strategy": inspection.extraction_strategy,
                    "report_path": str(report_path),
                },
                ensure_ascii=False,
            ),
            str(inspection.quality_tier),
            str(inspection.extraction_strategy),
            now_ts,
        )
    )

documents_schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("display_name", StringType(), False),
    StructField("quality_tier", StringType(), True),
    StructField("extraction_strategy", StringType(), True),
    StructField("usable_pages", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("avg_ocr_noise_score", DoubleType(), True),
    StructField("ingestion_ts", TimestampType(), True),
])

reports_schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("report_json", StringType(), True),
    StructField("quality_tier", StringType(), True),
    StructField("strategy_name", StringType(), True),
    StructField("report_ts", TimestampType(), True),
])

if not rows_documents:
    print("[INFO] No new documents found. Skipping stage 1 writes.")
    dbutils.notebook.exit("NO_NEW_DOCUMENTS")

df_documents = spark.createDataFrame(rows_documents, schema=documents_schema)
df_reports = spark.createDataFrame(rows_reports, schema=reports_schema)

documents_write_mode = "append" if spark.catalog.tableExists(documents_table) else "overwrite"
reports_write_mode = "append" if spark.catalog.tableExists(reports_table) else "overwrite"

(
    df_documents.write
    .mode(documents_write_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(documents_table)
)

(
    df_reports.write
    .mode(reports_write_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(reports_table)
)

print(f"[OK] wrote {documents_table}")
print(f"[OK] wrote {reports_table}")
