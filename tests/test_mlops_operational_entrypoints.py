def test_mlops_operational_entrypoints_import():
    from data_platform.mlops.run_mlops_drift_cycle import main as drift_main
    from data_platform.mlops.run_mlops_postprod_cycle import main as postprod_main
    from data_platform.mlops.run_mlops_retraining_cycle import main as retraining_main

    assert drift_main is not None
    assert postprod_main is not None
    assert retraining_main is not None
