-- =====================================================================================
-- TEMPLATE OFICIAL DE GRANTS
-- NAO ALTERAR A ESTRUTURA BASE
-- O DEV / PROJETO SO AJUSTA PRINCIPAL E ALVOS
-- =====================================================================================

GRANT USE CATALOG ON CATALOG <catalog> TO `<principal>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<principal>`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA <catalog>.<schema> TO `<principal>`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME <catalog>.<volume_schema>.<volume_name> TO `<principal>`;
