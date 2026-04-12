from data_platform.core.config_loader import load_yaml_config


def validate_official_readiness(config_path: str) -> dict:
    cfg = load_yaml_config(config_path)
    contract = load_yaml_config("config/platform_contracts/official_readiness_contract.yml")
    rules = contract["official_readiness"]

    missing_sections = [
        section for section in rules["required_top_level_sections"]
        if section not in cfg
    ]
    if missing_sections:
        return {
            "valid": False,
            "errors": [f"seções obrigatórias ausentes: {missing_sections}"],
        }

    errors = []

    for section_name in ["storage", "grants", "identity", "compute", "naming"]:
        section = cfg.get(section_name, {})
        required_fields = rules[section_name]["required_fields"]
        missing_fields = [field for field in required_fields if field not in section]
        if missing_fields:
            errors.append(f"{section_name}: campos ausentes {missing_fields}")

    storage_mode = cfg.get("storage", {}).get("mode")
    if storage_mode not in {"table", "path"}:
        errors.append("storage.mode deve ser table ou path")

    return {
        "valid": not errors,
        "errors": errors,
        "environment": cfg.get("environment"),
        "storage_mode": storage_mode,
    }
