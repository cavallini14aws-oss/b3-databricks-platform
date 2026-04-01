from importlib import import_module

from b3_platform.core.config_loader import load_yaml_config


def load_pipeline_definition(registry_path: str, pipeline_name: str) -> dict:
    registry = load_yaml_config(registry_path)

    pipelines = registry.get("pipelines", {})
    if pipeline_name not in pipelines:
        raise KeyError(f"Pipeline não encontrada no registry: {pipeline_name}")

    return pipelines[pipeline_name]


def resolve_pipeline_callable(registry_path: str, pipeline_name: str):
    definition = load_pipeline_definition(registry_path, pipeline_name)

    module_name = definition["module"]
    function_name = definition["function"]

    module = import_module(module_name)

    if not hasattr(module, function_name):
        raise AttributeError(
            f"Função '{function_name}' não encontrada no módulo '{module_name}'"
        )

    return getattr(module, function_name), definition
