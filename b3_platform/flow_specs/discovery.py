from pathlib import Path
import importlib
import pkgutil

from b3_platform.flow_specs.flow_catalog import load_flow_spec, flow_spec_to_dict


FLOW_SPECS_PROJECTS_PACKAGE = "b3_platform.flow_specs.projects"


def discover_flow_spec_modules(base_package: str = FLOW_SPECS_PROJECTS_PACKAGE) -> list[str]:
    package = importlib.import_module(base_package)
    package_path = Path(package.__file__).parent

    discovered_modules = []

    for module_info in pkgutil.walk_packages(
        path=[str(package_path)],
        prefix=f"{base_package}.",
    ):
        if module_info.ispkg:
            continue

        module_name = module_info.name
        discovered_modules.append(module_name)

    return sorted(discovered_modules)


def discover_flow_specs(base_package: str = FLOW_SPECS_PROJECTS_PACKAGE) -> list[dict]:
    discovered = []

    for module_name in discover_flow_spec_modules(base_package=base_package):
        flow_spec = load_flow_spec(module_name)
        payload = flow_spec_to_dict(flow_spec)
        payload["spec_module"] = module_name
        discovered.append(payload)

    return discovered
