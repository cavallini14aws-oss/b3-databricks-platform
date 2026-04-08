from pathlib import Path


def test_mlops_table_specs_exist():
    assert Path("sql/create_tables/mlops/001_create_tb_model_postprod_metrics.sql").exists()
    assert Path("sql/create_tables/mlops/002_create_tb_ml_alert_events.sql").exists()
    assert Path("sql/create_tables/mlops/003_create_tb_model_retraining_requests.sql").exists()
