CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_baseline (
    event_timestamp TIMESTAMP,
    env STRING,
    project STRING,
    model_name STRING,
    model_version STRING,
    baseline_name STRING,
    metric_name STRING,
    metric_value DOUBLE,
    run_id STRING
);
