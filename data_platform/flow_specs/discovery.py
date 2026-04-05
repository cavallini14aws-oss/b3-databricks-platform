from pathlib import Path

from data_platform.flow_specs.flow_catalog import safe_load_flow_spec


FLOW_SPECS_PROJECTS_PACKAGE = "data_platform.flow_specs.projects"


def discover_flow_spec_modules(base_package: str = FLOW_SPECS_PROJECTS_PACKAGE) -> list[str]:
    projects_dir = Path("data_platform/flow_specs/projects")

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
        payload = safe_load_flow_spec(module_name)
        discovered.append(payload)

    return discovered
