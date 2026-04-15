# Databricks notebook source
# MAGIC %pip install pymupdf pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime
from pathlib import Path
import hashlib
import os
import re
import sys
import unicodedata
import yaml

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
)

PROJECT_ROOT = os.path.abspath("..")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.document_catalog import load_document_catalog
from src.document_intelligence import inspect_document

import fitz


def normalize_text(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    normalized = normalized.replace("\u00a0", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def build_chunk_hash(
    document_id: str,
    file_name: str,
    page_number: int,
    chunk_text_normalized: str,
) -> str:
    payload = f"{document_id}|{file_name}|{page_number}|{chunk_text_normalized}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def extract_pdf_pages_native(pdf_path: str) -> list[dict]:
    pages = []
    doc = fitz.open(pdf_path)
    try:
        for idx, page in enumerate(doc, start=1):
            text = page.get_text("text") or ""
            pages.append(
                {
                    "page_number": idx,
                    "text": text,
                    "extraction_method": "native",
                }
            )
    finally:
        doc.close()
    return pages


def build_document_chunks(
    document_id: str,
    file_name: str,
    display_name: str,
    pages: list[dict],
    chunk_size: int = 1200,
    chunk_overlap: int = 200,
) -> list[dict]:
    chunks = []

    for page in pages:
        page_number = int(page["page_number"])
        page_text = page.get("text", "") or ""
        extraction_method = page.get("extraction_method", "native")

        normalized = normalize_text(page_text)
        if not normalized:
            continue

        start = 0
        index = 0

        while start < len(normalized):
            end = min(start + chunk_size, len(normalized))
            chunk_text = normalized[start:end].strip()

            if chunk_text:
                chunk_id = f"{document_id}_p{page_number}_c{index}"
                chunk_hash = build_chunk_hash(
                    document_id=document_id,
                    file_name=file_name,
                    page_number=page_number,
                    chunk_text_normalized=chunk_text,
                )

                chunks.append(
                    {
                        "document_id": document_id,
                        "file_name": file_name,
                        "display_name": display_name,
                        "chunk_id": chunk_id,
                        "page_number": page_number,
                        "chunk_text": chunk_text,
                        "chunk_text_normalized": chunk_text,
                        "chunk_hash": chunk_hash,
                        "page_extraction_method": extraction_method,
                    }
                )

            if end >= len(normalized):
                break

            start = max(end - chunk_overlap, start + 1)
            index += 1

    return chunks


tables_cfg_path = Path(PROJECT_ROOT) / "config" / "pdf_rag_tables.yml"
with open(tables_cfg_path, "r", encoding="utf-8") as f:
    tables_cfg = yaml.safe_load(f)

pages_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['pages']}"
chunks_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['chunks']}"

catalog = load_document_catalog("/Volumes/workspace/pdf_rag/raw_docs")

existing_chunk_files = set()
if spark.catalog.tableExists(chunks_table):
    existing_chunk_files = {
        row["file_name"]
        for row in spark.table(chunks_table).select("file_name").distinct().collect()
    }

catalog = [
    doc for doc in catalog
    if str(doc["source_file_name"]) not in existing_chunk_files
]

print(f"[INFO] new_documents_to_chunk={len(catalog)}")

rows_pages = []
rows_chunks = []

for doc in catalog:
    document_id = str(doc["document_id"])
    file_name = str(doc["source_file_name"])
    display_name = str(doc["display_name"])
    pdf_path = f"/Volumes/workspace/pdf_rag/raw_docs/{file_name}"

    inspection = inspect_document(pdf_path, file_name)
    pages = extract_pdf_pages_native(pdf_path)

    now_ts = datetime.utcnow()

    for page in pages:
        page_number = int(page["page_number"])
        page_text = page.get("text", "") or ""
        extraction_method = str(page.get("extraction_method", "native"))

        rows_pages.append(
            (
                document_id,
                file_name,
                page_number,
                page_text,
                extraction_method,
                str(inspection.quality_tier),
                True,
                now_ts,
            )
        )

    chunks = build_document_chunks(
        document_id=document_id,
        file_name=file_name,
        display_name=display_name,
        pages=pages,
    )

    for chunk in chunks:
        rows_chunks.append(
            (
                str(chunk["document_id"]),
                str(chunk["file_name"]),
                str(chunk["display_name"]),
                str(chunk["chunk_id"]),
                int(chunk["page_number"]),
                str(chunk["chunk_text"]),
                str(chunk["chunk_text_normalized"]),
                str(chunk["chunk_hash"]),
                str(chunk["page_extraction_method"]),
                str(inspection.quality_tier),
                now_ts,
            )
        )

pages_schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("page_number", IntegerType(), False),
    StructField("page_text", StringType(), True),
    StructField("extraction_method", StringType(), True),
    StructField("page_quality_tier", StringType(), True),
    StructField("is_usable", BooleanType(), True),
    StructField("ingestion_ts", TimestampType(), True),
])

chunks_schema = StructType([
    StructField("document_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("display_name", StringType(), False),
    StructField("chunk_id", StringType(), False),
    StructField("page_number", IntegerType(), False),
    StructField("chunk_text", StringType(), True),
    StructField("chunk_text_normalized", StringType(), True),
    StructField("chunk_hash", StringType(), True),
    StructField("page_extraction_method", StringType(), True),
    StructField("document_quality_tier", StringType(), True),
    StructField("ingestion_ts", TimestampType(), True),
])

if not rows_pages and not rows_chunks:
    print("[INFO] No new pages/chunks found. Skipping stage 2 writes.")
    dbutils.notebook.exit("NO_NEW_CHUNKS")

df_pages = spark.createDataFrame(rows_pages, schema=pages_schema)
df_chunks = spark.createDataFrame(rows_chunks, schema=chunks_schema)

(
    df_pages.write
    .mode("append")
    .option("overwriteSchema", "true")
    .saveAsTable(pages_table)
)

(
    df_chunks.write
    .mode("append")
    .option("overwriteSchema", "true")
    .saveAsTable(chunks_table)
)

print(f"[OK] wrote {pages_table}")
print(f"[OK] wrote {chunks_table}")
