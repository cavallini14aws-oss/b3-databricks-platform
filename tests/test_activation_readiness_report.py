from data_platform.core.activation_readiness_report import (
    build_activation_readiness_report,
)


def test_build_activation_readiness_report_returns_expected_shape(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_readiness_report.load_activation_control",
        lambda config_path: {
            "project": {
                "name": "b3_clientes_mlops",
                "owner": "mlops",
                "criticality": "high",
                "regulator_context": "b3",
            },
            "environments": {
                "dev": {
                    "go_live": {
                        "blockers": [],
                        "ready": True,
                    },
                    "notifications": {
                        "enabled": True,
                    },
                    "databricks": {
                        "enabled": True,
                    },
                    "jobs": {
                        "drift_cycle": {"enabled": True},
                    },
                },
                "prd": {
                    "go_live": {
                        "blockers": ["secrets_prd_pendentes"],
                        "ready": False,
                    },
                    "notifications": {
                        "enabled": True,
                    },
                    "databricks": {
                        "enabled": True,
                    },
                    "jobs": {
                        "drift_cycle": {"enabled": True},
                    },
                },
            },
        },
    )

    def fake_validate(env, config_path):
        if env == "dev":
            return {
                "ready": True,
                "errors": [],
                "warnings": [],
            }
        return {
            "ready": True,
            "errors": [],
            "warnings": ["notifications.email.smtp.ready=false"],
        }

    monkeypatch.setattr(
        "data_platform.core.activation_readiness_report.validate_activation_environment",
        fake_validate,
    )

    report = build_activation_readiness_report()

    assert report["project"] == "b3_clientes_mlops"
    assert report["owner"] == "mlops"
    assert report["criticality"] == "high"
    assert report["regulator_context"] == "b3"
    assert report["ready_environments"] == 1
    assert report["total_environments"] == 2
    assert report["readiness_score"] == 0.5
    assert report["environments"]["dev"]["ready"] is True
    assert report["environments"]["prd"]["ready"] is False
    assert report["environments"]["prd"]["blockers"] == ["secrets_prd_pendentes"]
