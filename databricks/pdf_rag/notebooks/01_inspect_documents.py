# Databricks notebook source
import json
import os
from datetime import datetime

import yaml

from pipelines.examples.pdf_rag.backend.services.document_catalog import load_document_catalog
from pipelines.examples.pdf_rag.backend.services.document_intelligence import inspect_document
from pipelines.examples.pdf_rag.backend.services.document_quality_report import write_document_quality_report

with open("databricks/pdf_rag/config/pdf_rag_tables.yml", "r", encoding="utf-8") as f:
    tables_cfg = yaml.safe_load(f)

documents_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['documents']}"
reports_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['quality_reports']}"

catalog = load_document_catalog()
rows_documents = []
rows_reports = []

for doc in catalog:
    pdf_path = f"pipelines/examples/pdf_rag/backend/data/raw/{doc['source_file_name']}"
    inspection = inspect_document(pdf_path, doc["source_file_name"])
    report_path = write_document_quality_report(inspection)

    rows_documents.append(
        (
            doc["document_id"],
            doc["source_file_name"],
            doc["display_name"],
            inspection.quality_tier,
            inspection.extraction_strategy,
            inspection.usable_pages,
            inspection.total_pages,
            float(inspection.avg_ocr_noise_score),
            datetime.utcnow(),
        )
    )

    report_payload = {
        "file_name": inspection.file_name,
        "total_pages": inspection.total_pages,
        "usable_pages": inspection.usable_pages,
        "unusable_pages": inspection.unusable_pages,
        "avg_ocr_noise_score": inspection.avg_ocr_noise_score,
        "quality_tier": inspection.quality_tier,
        "extraction_strategy": inspection.extraction_strategy,
        "report_path": str(report_path),
    }

    rows_reports.append(
        (
            doc["document_id"],
            doc["source_file_name"],
            json.dumps(report_payload, ensure_ascii=False),
            inspection.quality_tier,
            inspection.extraction_strategy,
            datetime.utcnow(),
        )
    )

df_documents = spark.createDataFrame(
    rows_documents,
    [
        "document_id",
        "file_name",
        "display_name",
        "quality_tier",
        "extraction_strategy",
        "usable_pages",
        "total_pages",
        "avg_ocr_noise_score",
        "ingestion_ts",
    ],
)

df_reports = spark.createDataFrame(
    rows_reports,
    [
        "document_id",
        "file_name",
        "report_json",
        "quality_tier",
        "strategy_name",
        "report_ts",
    ],
)

df_documents.write.mode("overwrite").saveAsTable(documents_table)
df_reports.write.mode("overwrite").saveAsTable(reports_table)

print(f"[OK] wrote {documents_table}")
print(f"[OK] wrote {reports_table}")
