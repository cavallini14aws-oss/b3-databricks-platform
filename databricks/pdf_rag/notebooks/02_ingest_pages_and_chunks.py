# Databricks notebook source
from datetime import datetime

import yaml

from pipelines.examples.pdf_rag.backend.services.document_catalog import load_document_catalog
from pipelines.examples.pdf_rag.backend.services.document_ingestion_strategy import choose_ingestion_strategy
from pipelines.examples.pdf_rag.backend.services.document_intelligence import inspect_document
from pipelines.examples.pdf_rag.backend.services.hybrid_pdf_extractor import extract_pdf_pages_hybrid
from pipelines.examples.pdf_rag.backend.services.pdf_service import build_document_chunks, extract_pdf_pages

with open("databricks/pdf_rag/config/pdf_rag_tables.yml", "r", encoding="utf-8") as f:
    tables_cfg = yaml.safe_load(f)

pages_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['pages']}"
chunks_table = f"{tables_cfg['catalog']}.{tables_cfg['schema']}.{tables_cfg['tables']['chunks']}"

catalog = load_document_catalog()
rows_pages = []
rows_chunks = []

for doc in catalog:
    pdf_path = f"pipelines/examples/pdf_rag/backend/data/raw/{doc['source_file_name']}"
    inspection = inspect_document(pdf_path, doc["source_file_name"])
    strategy = choose_ingestion_strategy(inspection)

    if strategy.use_ocr_fallback:
        pages = extract_pdf_pages_hybrid(
            pdf_path,
            doc["source_file_name"],
            ocr_lang="eng",
            max_ocr_pages=12,
            ocr_page_limit=40,
        )
    else:
        native_pages = extract_pdf_pages(pdf_path)
        pages = [
            {
                "page_number": p["page_number"],
                "text": p["text"],
                "extraction_method": "native",
            }
            for p in native_pages
        ]

    quality_by_page = {p.page_number: p for p in inspection.page_reports}

    for page in pages:
        page_report = quality_by_page.get(page["page_number"])
        rows_pages.append(
            (
                doc["document_id"],
                doc["source_file_name"],
                page["page_number"],
                page["text"],
                page["extraction_method"],
                inspection.quality_tier,
                page_report.is_usable if page_report else True,
                datetime.utcnow(),
            )
        )

    chunks = build_document_chunks(
        pdf_path=pdf_path,
        file_name=doc["source_file_name"],
        pages=pages,
    )

    for chunk in chunks:
        rows_chunks.append(
            (
                doc["document_id"],
                doc["source_file_name"],
                doc["display_name"],
                chunk["chunk_id"],
                chunk["page_number"],
                chunk["chunk_text"],
                chunk["chunk_text_normalized"],
                chunk["chunk_hash"],
                chunk.get("page_extraction_method", "native"),
                inspection.quality_tier,
                datetime.utcnow(),
            )
        )

df_pages = spark.createDataFrame(
    rows_pages,
    [
        "document_id",
        "file_name",
        "page_number",
        "page_text",
        "extraction_method",
        "page_quality_tier",
        "is_usable",
        "ingestion_ts",
    ],
)

df_chunks = spark.createDataFrame(
    rows_chunks,
    [
        "document_id",
        "file_name",
        "display_name",
        "chunk_id",
        "page_number",
        "chunk_text",
        "chunk_text_normalized",
        "chunk_hash",
        "page_extraction_method",
        "document_quality_tier",
        "ingestion_ts",
    ],
)

df_pages.write.mode("overwrite").saveAsTable(pages_table)
df_chunks.write.mode("overwrite").saveAsTable(chunks_table)

print(f"[OK] wrote {pages_table}")
print(f"[OK] wrote {chunks_table}")
