from pathlib import Path


def is_local_ai_mode(config: dict) -> bool:
    return config.get("storage", {}).get("mode", "table") == "path"


def ai_local_base_path(config: dict, project: str) -> str:
    return config.get("storage", {}).get("base_path", f"artifacts/ai_local/{project}")


def ai_local_dataset_path(config: dict, project: str, dataset: str) -> str:
    base = ai_local_base_path(config, project)
    return str(Path(base) / dataset)
