def test_clientes_postprod_pipelines_import():
    from pipelines.examples.clientes.ml.prepare_clientes_postprod_labels import (
        run_prepare_clientes_postprod_labels,
    )
    from pipelines.examples.clientes.ml.evaluate_clientes_postprod import (
        run_evaluate_clientes_postprod,
    )

    assert run_prepare_clientes_postprod_labels is not None
    assert run_evaluate_clientes_postprod is not None
