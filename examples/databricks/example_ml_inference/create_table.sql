CREATE TABLE IF NOT EXISTS clientes_${env}.ml.tb_exemplo_inference (
  id_cliente      STRING COMMENT 'Identificador do cliente ou entidade inferida',
  vl_score        DOUBLE COMMENT 'Score calculado pelo modelo',
  ds_faixa_score  STRING COMMENT 'Faixa categórica derivada do score',
  ingestion_date  TIMESTAMP COMMENT 'Data/hora de ingestão da inferência',
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
