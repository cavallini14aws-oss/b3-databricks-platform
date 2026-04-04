CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_confusion_matrix (
    event_timestamp TIMESTAMP,
    env STRING,
    project STRING,
    model_name STRING,
    model_version STRING,
    label DOUBLE,
    prediction DOUBLE,
    record_count BIGINT,
    run_id STRING
);
