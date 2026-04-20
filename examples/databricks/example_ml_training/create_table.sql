CREATE TABLE IF NOT EXISTS clientes_${env}.ml.tb_exemplo_training_runs (
  id_run          STRING COMMENT 'Identificador do treino/execução',
  nm_modelo       STRING COMMENT 'Nome lógico do modelo treinado',
  vl_metric_auc   DOUBLE COMMENT 'Métrica AUC do treino avaliado',
  vl_metric_f1    DOUBLE COMMENT 'Métrica F1 do treino avaliado',
  ingestion_date  TIMESTAMP COMMENT 'Data/hora de ingestão do registro de treino',
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
