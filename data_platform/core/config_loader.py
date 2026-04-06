from pathlib import Path

import yaml


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_project_path(relative_path: str) -> Path:
    candidate = Path(relative_path)

    if candidate.is_absolute():
        return candidate

    return get_project_root() / candidate


def load_yaml_config(config_path: str) -> dict:
    path = resolve_project_path(config_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {config_path}"
        )

    with path.open("r", encoding="utf-8") as file:
        content = yaml.safe_load(file)

    return content or {}
