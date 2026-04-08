from data_platform.core.go_live_report import build_go_live_report


def test_build_go_live_report_returns_expected_shape(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.go_live_report.load_activation_control",
        lambda config_path: {
            "project": {
                "name": "b3_clientes_mlops",
                "owner": "mlops",
                "criticality": "high",
                "regulator_context": "b3",
            },
            "environments": {
                "dev": {
                    "go_live": {"ready": False, "blockers": []},
                    "databricks": {"enabled": True},
                    "notifications": {
                        "enabled": True,
                        "severity_min": "WARNING",
                        "email": {"enabled": True},
                        "slack": {"enabled": False},
                        "teams": {"enabled": False},
                    },
                    "retraining": {},
                    "thresholds": {},
                    "retention": {},
                    "access_control": {},
                    "jobs": {},
                    "cicd": {},
                }
            },
        },
    )
    monkeypatch.setattr(
        "data_platform.core.go_live_report.validate_activation_environment",
        lambda env, config_path: {
            "ready": True,
            "errors": [],
            "warnings": [],
        },
    )

    report = build_go_live_report()

    assert report["project"] == "b3_clientes_mlops"
    assert report["owner"] == "mlops"
    assert report["criticality"] == "high"
    assert report["regulator_context"] == "b3"
    assert report["environments"]["dev"]["ready"] is True
    assert report["environments"]["dev"]["notifications"]["email_enabled"] is True
