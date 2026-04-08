CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_retraining_requests (
  event_timestamp TIMESTAMP,
  env STRING,
  project STRING,
  model_name STRING,
  model_version STRING,
  trigger_type STRING,
  trigger_source STRING,
  trigger_severity STRING,
  reason STRING,
  request_status STRING,
  requested_by STRING,
  run_id STRING
)
USING DELTA;
