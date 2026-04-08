from data_platform.core.activation_control import get_activation_access_control_config
from data_platform.core.activation_validator import validate_activation_environment


def test_get_activation_access_control_config(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_control.load_yaml_config",
        lambda path: {
            "environments": {
                "prd": {
                    "access_control": {
                        "promote_roles": ["mlops"],
                    }
                }
            }
        },
    )

    cfg = get_activation_access_control_config("prd")
    assert cfg["promote_roles"] == ["mlops"]


def test_validate_activation_environment_requires_acl(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_validator.get_activation_environment_config",
        lambda env, config_path: {
            "databricks": {
                "workspace_host_key": "PRD_WORKSPACE_HOST",
                "databricks_token_key": "PRD_DATABRICKS_TOKEN",
                "cluster_id_key": "PRD_CLUSTER_ID",
                "secret_scope": "keyvault-prd-datahub",
            },
            "notifications": {
                "enabled": False,
            },
            "jobs": {},
            "access_control": {},
            "go_live": {"blockers": []},
        },
    )

    result = validate_activation_environment("prd")
    assert result["ready"] is False
    assert any("access_control.promote_roles vazio" in err for err in result["errors"])
