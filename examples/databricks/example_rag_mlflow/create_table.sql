CREATE TABLE IF NOT EXISTS clientes_${env}.slv.tb_exemplo_rag_mlflow_chunks (
  id_documento    STRING COMMENT 'Identificador lógico do documento',
  nm_arquivo      STRING COMMENT 'Nome do arquivo/documento processado',
  ds_chunk        STRING COMMENT 'Texto do chunk gerado a partir do documento',
  nr_chunk        INT COMMENT 'Número sequencial do chunk no documento',
  ds_variant      STRING COMMENT 'Variant do workload RAG (ex: mlflow)',
  ingestion_date  TIMESTAMP COMMENT 'Data/hora de ingestão do chunk',
  update_date     TIMESTAMP COMMENT 'Data/hora da última atualização do chunk'
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
