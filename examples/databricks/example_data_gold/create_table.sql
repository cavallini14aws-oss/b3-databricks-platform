CREATE TABLE IF NOT EXISTS clientes_${env}.gld.tb_exemplo_gold (
  dt_referencia   STRING COMMENT 'Data de referência da visão consolidada',
  qt_clientes     BIGINT COMMENT 'Quantidade distinta de clientes na referência',
  ingestion_date  TIMESTAMP COMMENT 'Data/hora de ingestão da camada Gold',
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
