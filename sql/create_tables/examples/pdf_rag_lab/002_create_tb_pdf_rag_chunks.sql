-- =====================================================================================
-- TEMPLATE OFICIAL DE CREATE TABLE
-- NAO ALTERAR A ESTRUTURA BASE DESTE TEMPLATE
-- =====================================================================================

-- TABELA: <catalog>.<schema>.tb_pdf_rag_chunks
-- DESCRICAO: Tabela de chunks textuais gerados a partir dos documentos do PDF RAG Lab.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.tb_pdf_rag_chunks (
    document_id STRING COMMENT 'Identificador do documento de origem.',
    chunk_id STRING COMMENT 'Identificador único do chunk.',
    chunk_index INT COMMENT 'Posição sequencial do chunk no documento.',
    page_number INT COMMENT 'Página do PDF de onde o chunk foi extraído.',
    chunk_text STRING COMMENT 'Texto do chunk recuperável.',
    project STRING COMMENT 'Projeto associado ao chunk.'
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
