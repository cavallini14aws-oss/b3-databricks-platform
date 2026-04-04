from b3_platform.flow_specs.flow_spec import FlowSpec, VALID_FLOW_TYPES


REQUIRED_TAGS = {
    "owner",
    "criticality",
    "schedule",
}


def validate_flow_spec(flow_spec: FlowSpec) -> None:
    if not flow_spec.flow_name.strip():
        raise ValueError("flow_name é obrigatório.")

    if flow_spec.flow_type not in VALID_FLOW_TYPES:
        raise ValueError(
            f"flow_type inválido: {flow_spec.flow_type}. "
            f"Valores aceitos: {sorted(VALID_FLOW_TYPES)}"
        )

    if not flow_spec.project.strip():
        raise ValueError("project é obrigatório.")

    if not flow_spec.domain.strip():
        raise ValueError("domain é obrigatório.")

    if not flow_spec.description.strip():
        raise ValueError("description é obrigatório.")

    if not flow_spec.entrypoint.strip():
        raise ValueError("entrypoint é obrigatório.")

    if not flow_spec.callable_name.strip():
        raise ValueError("callable_name é obrigatório.")

    missing_tags = REQUIRED_TAGS - set(flow_spec.tags.keys())
    if missing_tags:
        raise ValueError(
            f"tags obrigatórias ausentes em {flow_spec.flow_name}: {sorted(missing_tags)}"
        )
