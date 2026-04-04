import importlib
from dataclasses import asdict

from b3_platform.flow_specs.flow_spec import FlowSpec
from b3_platform.flow_specs.flow_validator import validate_flow_spec


def load_flow_spec(spec_module: str) -> FlowSpec:
    module = importlib.import_module(spec_module)

    if not hasattr(module, "FLOW_SPEC"):
        raise AttributeError(f"O módulo {spec_module} não possui FLOW_SPEC.")

    flow_spec = module.FLOW_SPEC
    validate_flow_spec(flow_spec)
    return flow_spec


def load_flow_callable(spec_module: str):
    module = importlib.import_module(spec_module)

    if not hasattr(module, "FLOW_SPEC"):
        raise AttributeError(f"O módulo {spec_module} não possui FLOW_SPEC.")

    flow_spec = module.FLOW_SPEC
    validate_flow_spec(flow_spec)

    if not hasattr(module, flow_spec.callable_name):
        raise AttributeError(
            f"O módulo {spec_module} não possui callable {flow_spec.callable_name}."
        )

    return flow_spec, getattr(module, flow_spec.callable_name)


def safe_load_flow_spec(spec_module: str) -> dict:
    try:
        flow_spec = load_flow_spec(spec_module)
        payload = flow_spec_to_dict(flow_spec)
        payload["spec_module"] = spec_module
        payload["load_status"] = "SUCCESS"
        payload["load_error"] = None
        return payload
    except Exception as exc:
        return {
            "spec_module": spec_module,
            "load_status": "ERROR",
            "load_error": f"{type(exc).__name__}: {exc}",
        }


def flow_spec_to_dict(flow_spec: FlowSpec) -> dict:
    return asdict(flow_spec)
