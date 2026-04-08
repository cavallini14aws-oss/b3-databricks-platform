CREATE TABLE IF NOT EXISTS clientes_mlops.tb_ml_alert_events (
  event_timestamp TIMESTAMP,
  env STRING,
  project STRING,
  model_name STRING,
  model_version STRING,
  run_id STRING,
  source_component STRING,
  metric_name STRING,
  entity_name STRING,
  baseline_value DOUBLE,
  current_value DOUBLE,
  severity STRING,
  message STRING,
  notification_status STRING,
  notification_error STRING
)
USING DELTA;
