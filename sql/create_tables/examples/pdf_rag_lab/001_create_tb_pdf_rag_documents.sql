-- =====================================================================================
-- TEMPLATE OFICIAL DE CREATE TABLE
-- NAO ALTERAR A ESTRUTURA BASE DESTE TEMPLATE
-- =====================================================================================

-- TABELA: <catalog>.<schema>.tb_pdf_rag_documents
-- DESCRICAO: Tabela de documentos ingeridos pelo PDF RAG Lab.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.tb_pdf_rag_documents (
    document_id STRING COMMENT 'Identificador único do documento.',
    file_name STRING COMMENT 'Nome do arquivo PDF original.',
    source_path STRING COMMENT 'Caminho de origem do arquivo.',
    document_title STRING COMMENT 'Título identificado ou derivado do documento.',
    ingestion_ts TIMESTAMP COMMENT 'Data e hora de ingestão do documento.',
    project STRING COMMENT 'Projeto associado ao documento.'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.deletionVectors' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7'
);
