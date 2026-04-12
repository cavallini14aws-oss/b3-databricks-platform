-- =====================================================================================
-- TEMPLATE OFICIAL DE CREATE TABLE
-- NAO ALTERAR A ESTRUTURA BASE DESTE TEMPLATE
-- =====================================================================================

-- TABELA: <catalog>.<schema>.tb_pdf_rag_queries
-- DESCRICAO: Tabela de consultas e respostas do PDF RAG Lab para rastreabilidade e análise.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.tb_pdf_rag_queries (
    query_id STRING COMMENT 'Identificador único da consulta.',
    question_text STRING COMMENT 'Pergunta enviada pelo usuário.',
    retrieved_chunk_count INT COMMENT 'Quantidade de chunks recuperados.',
    response_text STRING COMMENT 'Resposta final gerada pelo LLM.',
    llm_model STRING COMMENT 'Modelo LLM utilizado na resposta.',
    created_ts TIMESTAMP COMMENT 'Data e hora da consulta.',
    project STRING COMMENT 'Projeto associado à consulta.'
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
