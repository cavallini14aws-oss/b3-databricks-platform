from data_platform.core.activation_control import get_activation_environment_config


def get_hml_rehearsal_blockers(
    config_path: str = "config/activation/operational_control.yml",
) -> list[str]:
    cfg = get_activation_environment_config("hml", config_path=config_path)
    return cfg.get("go_live", {}).get("blockers", [])


def is_hml_rehearsal_ready(
    config_path: str = "config/activation/operational_control.yml",
) -> bool:
    blockers = get_hml_rehearsal_blockers(config_path=config_path)
    return len(blockers) == 0
