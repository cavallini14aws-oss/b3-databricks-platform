CREATE SCHEMA IF NOT EXISTS workspace.pdf_rag;

CREATE TABLE IF NOT EXISTS workspace.pdf_rag.pdf_documents (
  document_id STRING,
  file_name STRING,
  display_name STRING,
  quality_tier STRING,
  extraction_strategy STRING,
  usable_pages INT,
  total_pages INT,
  avg_ocr_noise_score DOUBLE,
  ingestion_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.pdf_rag.pdf_pages (
  document_id STRING,
  file_name STRING,
  page_number INT,
  page_text STRING,
  extraction_method STRING,
  page_quality_tier STRING,
  is_usable BOOLEAN,
  ingestion_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.pdf_rag.pdf_chunks (
  document_id STRING,
  file_name STRING,
  display_name STRING,
  chunk_id STRING,
  page_number INT,
  chunk_text STRING,
  chunk_text_normalized STRING,
  chunk_hash STRING,
  page_extraction_method STRING,
  document_quality_tier STRING,
  ingestion_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.pdf_rag.pdf_chunk_embeddings (
  chunk_id STRING,
  document_id STRING,
  file_name STRING,
  embedding ARRAY<DOUBLE>,
  embedding_model STRING,
  embedding_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.pdf_rag.pdf_document_quality_reports (
  document_id STRING,
  file_name STRING,
  report_json STRING,
  quality_tier STRING,
  strategy_name STRING,
  report_ts TIMESTAMP
) USING DELTA;


ALTER TABLE workspace.pdf_rag.pdf_chunks
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
