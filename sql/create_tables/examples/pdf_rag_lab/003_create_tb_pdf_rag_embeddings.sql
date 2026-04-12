-- =====================================================================================
-- TEMPLATE OFICIAL DE CREATE TABLE
-- NAO ALTERAR A ESTRUTURA BASE DESTE TEMPLATE
-- =====================================================================================

-- TABELA: <catalog>.<schema>.tb_pdf_rag_embeddings
-- DESCRICAO: Tabela de embeddings gerados para os chunks do PDF RAG Lab.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.tb_pdf_rag_embeddings (
    document_id STRING COMMENT 'Identificador do documento de origem.',
    chunk_id STRING COMMENT 'Identificador do chunk vetorizado.',
    chunk_index INT COMMENT 'Posição sequencial do chunk.',
    embedding ARRAY<FLOAT> COMMENT 'Vetor numérico do chunk.',
    embedding_model STRING COMMENT 'Modelo utilizado para gerar o embedding.',
    project STRING COMMENT 'Projeto associado ao embedding.'
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
