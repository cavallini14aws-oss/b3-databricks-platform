CREATE TABLE IF NOT EXISTS clientes_${env}.brz.tb_exemplo_bronze (
  id_cliente_origem STRING COMMENT 'Identificador recebido da origem',
  nm_cliente        STRING COMMENT 'Nome do cliente recebido na carga de origem',
  ds_email          STRING COMMENT 'E-mail do cliente recebido na origem',
  nr_telefone       STRING COMMENT 'Telefone do cliente recebido na origem',
  dt_referencia     STRING COMMENT 'Data de referência da carga na origem',
  ingestion_date    TIMESTAMP COMMENT 'Data/hora de ingestão da carga',
  update_date       TIMESTAMP COMMENT 'Data/hora da última atualização do registro'
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
