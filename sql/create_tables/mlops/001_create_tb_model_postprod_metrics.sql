CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_postprod_metrics (
  event_timestamp TIMESTAMP,
  env STRING,
  project STRING,
  model_name STRING,
  model_version STRING,
  metric_name STRING,
  metric_value DOUBLE,
  window_start STRING,
  window_end STRING,
  run_id STRING
)
USING DELTA;
