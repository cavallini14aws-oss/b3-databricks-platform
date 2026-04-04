CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_evaluation (
    event_timestamp TIMESTAMP,
    env STRING,
    project STRING,
    model_name STRING,
    model_version STRING,
    metric_name STRING,
    metric_value DOUBLE,
    run_id STRING
);
