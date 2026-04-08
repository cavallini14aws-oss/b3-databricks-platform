def test_postprod_reconciliation_module_imports():
    from data_platform.mlops.postprod_reconciliation import (
        apply_time_window,
        build_postprod_reconciliation_df,
        reconcile_postprod_from_tables,
    )

    assert apply_time_window is not None
    assert build_postprod_reconciliation_df is not None
    assert reconcile_postprod_from_tables is not None
