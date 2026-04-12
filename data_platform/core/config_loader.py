from pathlib import Path
from importlib import resources
import yaml


def resolve_project_path(relative_path: str) -> Path:
    return Path(relative_path)


def _load_packaged_yaml(config_path: str):
    resource_root = resources.files("data_platform.resources")
    packaged = resource_root.joinpath(config_path)

    if not packaged.is_file():
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {config_path}"
        )

    with packaged.open("r", encoding="utf-8") as file:
        content = yaml.safe_load(file)

    return content or {}


def load_yaml_config(config_path: str) -> dict:
    path = resolve_project_path(config_path)

    if path.exists():
        with path.open("r", encoding="utf-8") as file:
            content = yaml.safe_load(file)
        return content or {}

    return _load_packaged_yaml(config_path)
