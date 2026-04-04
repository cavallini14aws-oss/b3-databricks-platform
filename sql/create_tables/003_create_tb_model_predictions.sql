CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_predictions (
    event_timestamp TIMESTAMP,
    env STRING,
    project STRING,
    model_name STRING,
    model_version STRING,
    id_cliente BIGINT,
    segmento_dominante STRING,
    source_type_dominante STRING,
    label DOUBLE,
    prediction DOUBLE,
    dataset_split STRING,
    run_id STRING
);
