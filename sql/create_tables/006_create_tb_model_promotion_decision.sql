CREATE TABLE IF NOT EXISTS clientes_mlops.tb_model_promotion_decision (
    event_timestamp TIMESTAMP,
    env STRING,
    project STRING,
    model_name STRING,
    model_version STRING,
    source_env STRING,
    target_env STRING,
    approved BOOLEAN,
    reason STRING,
    accuracy DOUBLE,
    f1 DOUBLE,
    auc DOUBLE,
    tests_passed BOOLEAN,
    manual_approval BOOLEAN,
    run_id STRING
);
