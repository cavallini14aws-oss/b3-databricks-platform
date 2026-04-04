from b3_platform.flow_specs.flow_spec import FlowSpec
from pipelines.examples.clientes.ml.run_clientes_ml_end_to_end import (
    run_clientes_ml_end_to_end,
)


FLOW_SPEC = FlowSpec(
    flow_name="ml_clientes_end_to_end",
    flow_type="ml",
    project="clientes",
    domain="clientes",
    layer="ml",
    description="Fluxo ML end-to-end do domínio clientes",
    entrypoint=(
        "b3_platform.flow_specs.projects.clientes.ml_clientes_end_to_end."
        "run_clientes_ml_end_to_end"
    ),
    callable_name="run_clientes_ml_end_to_end",
    tags={
        "owner": "time_clientes",
        "criticality": "alta",
        "schedule": "manual",
    },
    enabled=True,
)
