import importlib
from dataclasses import asdict

from data_platform.flow_specs.flow_spec import FlowSpec
from data_platform.flow_specs.flow_validator import validate_flow_spec


def load_flow_spec(spec_module: str) -> FlowSpec:
    module = importlib.import_module(spec_module)

    if not hasattr(module, "FLOW_SPEC"):
        raise AttributeError(f"O módulo {spec_module} não possui FLOW_SPEC.")

    flow_spec = module.FLOW_SPEC
    validate_flow_spec(flow_spec)
    return flow_spec


def _resolve_callable_from_entrypoint(entrypoint: str):
    if "." not in entrypoint:
        raise ValueError(f"Entrypoint inválido: {entrypoint}")

    module_name, callable_name = entrypoint.rsplit(".", 1)
    module = importlib.import_module(module_name)

    if not hasattr(module, callable_name):
        raise AttributeError(
            f"O módulo {module_name} não possui callable {callable_name}."
        )

    return getattr(module, callable_name)


def load_flow_callable(spec_module: str):
    flow_spec = load_flow_spec(spec_module)
    flow_callable = _resolve_callable_from_entrypoint(flow_spec.entrypoint)
    return flow_spec, flow_callable


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
