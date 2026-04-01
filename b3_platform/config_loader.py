import json
from pathlib import Path


def load_json_config(config_path: str) -> dict:
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)