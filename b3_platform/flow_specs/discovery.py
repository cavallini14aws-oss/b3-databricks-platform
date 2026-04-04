from pathlib import Path

from b3_platform.flow_specs.flow_catalog import load_flow_spec, flow_spec_to_dict


FLOW_SPECS_PROJECTS_PACKAGE = "b3_platform.flow_specs.projects"


def discover_flow_spec_modules(base_package: str = FLOW_SPECS_PROJECTS_PACKAGE) -> list[str]:
    projects_dir = Path("b3_platform/flow_specs/projects")

    if not projects_dir.exists():
        return []

    discovered_modules = []

    for py_file in sorted(projects_dir.rglob("*.py")):
        if py_file.name == "__init__.py":
            continue

        relative_path = py_file.with_suffix("")
        module_name = ".".join(relative_path.parts)
        discovered_modules.append(module_name)

    return discovered_modules


def discover_flow_specs(base_package: str = FLOW_SPECS_PROJECTS_PACKAGE) -> list[dict]:
    discovered = []

    for module_name in discover_flow_spec_modules(base_package=base_package):
        flow_spec = load_flow_spec(module_name)
        payload = flow_spec_to_dict(flow_spec)
        payload["spec_module"] = module_name
        discovered.append(payload)

    return discovered
