from b3_platform.flow_specs.flow_spec import FlowSpec


FLOW_SPEC = FlowSpec(
    flow_name="flow_exemplo_template",
    flow_type="ml",
    project="clientes",
    domain="exemplo",
    layer="ml",
    description="Fluxo de exemplo para template declarativo",
    entrypoint="b3_platform.flow_specs.templates.example_flow_spec.run",
    callable_name="run",
    tags={
        "owner": "time_exemplo",
        "criticality": "media",
        "schedule": "manual",
    },
    enabled=True,
)


def run(spark, project: str, use_catalog: bool = False, config_path: str | None = None):
    print(
        {
            "message": "Fluxo template executado com sucesso.",
            "project": project,
            "use_catalog": use_catalog,
            "config_path": config_path,
        }
    )
