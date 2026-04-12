-- =====================================================================================
-- TEMPLATE OFICIAL DE CREATE TABLE
-- NAO ALTERAR A ESTRUTURA BASE DESTE TEMPLATE
-- =====================================================================================

-- TABELA: <catalog>.<schema>.tb_example_ai_knowledge
-- DESCRICAO: Tabela de knowledge base de exemplo para pipeline AI/RAG.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.tb_example_ai_knowledge (
    document_id STRING COMMENT 'Identificador único do documento.',
    document_text STRING COMMENT 'Conteúdo textual do documento.',
    source_type STRING COMMENT 'Tipo da origem documental.',
    project STRING COMMENT 'Projeto associado ao registro.'
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
