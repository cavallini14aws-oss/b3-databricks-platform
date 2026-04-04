CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_registry (
    event_timestamp TIMESTAMP,
    env STRING,
    project STRING,
    model_name STRING,
    model_version STRING,
    algorithm STRING,
    run_id STRING,
    status STRING,
    artifact_path STRING
);
