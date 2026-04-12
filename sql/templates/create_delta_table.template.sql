-- =====================================================================================
-- TEMPLATE OFICIAL DE CREATE TABLE
-- NAO ALTERAR A ESTRUTURA BASE DESTE TEMPLATE
-- O DEV PODE EDITAR:
--   - nome da tabela
--   - colunas
--   - comentarios
--   - tags
--   - pii
-- O BLOCO DELTA FIXO NAO DEVE SER ALTERADO
-- =====================================================================================

-- TABELA: <catalog>.<schema>.<table_name>
-- DESCRICAO: <table_description>

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table_name> (
    -- <column_name> <data_type> COMMENT '<column_description>'
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

-- TAGS DE TABELA (preencher conforme politica do projeto)
-- Exemplo:
-- ALTER TABLE <catalog>.<schema>.<table_name> SET TAGS (
--   'domain' = '<domain>',
--   'layer' = '<layer>',
--   'contains_pii' = '<true_or_false>'
-- );

-- COMENTARIOS / TAGS / PII DE COLUNAS
-- Exemplo:
-- ALTER TABLE <catalog>.<schema>.<table_name>
-- ALTER COLUMN <column_name> COMMENT '<column_description>';
--
-- TAGS e regras complementares seguem o contrato TableSpec da plataforma.
