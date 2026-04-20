CREATE TABLE IF NOT EXISTS clientes_${env}.slv.tb_exemplo_silver (
  id_cliente      STRING COMMENT 'Identificador padronizado do cliente',
  nm_cliente      STRING COMMENT 'Nome padronizado do cliente',
  ds_email        STRING COMMENT 'E-mail padronizado do cliente',
  nr_telefone     STRING COMMENT 'Telefone padronizado do cliente',
  dt_referencia   STRING COMMENT 'Data de referência do dado',
  ingestion_date  TIMESTAMP COMMENT 'Data/hora de ingestão da camada Silver',
  update_date     TIMESTAMP COMMENT 'Data/hora da última atualização do registro'
)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7'
);
