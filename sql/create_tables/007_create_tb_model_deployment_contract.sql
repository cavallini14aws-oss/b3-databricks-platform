CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_deployment_contract (
    event_timestamp TIMESTAMP,
    env STRING,
    project STRING,
    model_name STRING,
    model_version STRING,
    source_env STRING,
    target_env STRING,
    target_cluster_key STRING,
    target_workspace_root STRING,
    target_timeout_seconds INT,
    target_max_retries INT,
    run_id STRING
);
