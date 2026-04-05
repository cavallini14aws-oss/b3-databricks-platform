from pathlib import Path

import yaml


def load_yaml_config(config_path: str) -> dict:
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {config_path}"
        )

    with path.open("r", encoding="utf-8") as file:
        content = yaml.safe_load(file)

    return content or {}
