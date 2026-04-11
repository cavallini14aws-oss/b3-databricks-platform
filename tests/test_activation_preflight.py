from data_platform.core.activation_preflight import run_activation_preflight


def test_run_activation_preflight_returns_block_when_blockers_exist(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_preflight.build_activation_readiness_report",
        lambda: {
            "environments": {
                "dev": {
                    "errors": [],
                    "warnings": [],
                    "blockers": [],
                },
                "prd": {
                    "errors": [],
                    "warnings": [],
                    "blockers": ["secrets_prd_pendentes"],
                },
            }
        },
    )
    monkeypatch.setattr(
        "data_platform.core.activation_preflight.build_go_live_report",
        lambda: {
            "environments": {
                "dev": {
                    "errors": [],
                    "warnings": [],
                    "go_live": {"blockers": []},
                },
                "prd": {
                    "errors": [],
                    "warnings": [],
                    "go_live": {"blockers": ["acl_prd_pendente"]},
                },
            }
        },
    )

    result = run_activation_preflight()

    assert result["status"] == "BLOCK"
    assert result["environments"]["prd"]["status"] == "BLOCK"


def test_run_activation_preflight_returns_warn_when_only_warnings_exist(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.activation_preflight.build_activation_readiness_report",
        lambda: {
            "environments": {
                "dev": {
                    "errors": [],
                    "warnings": ["smtp.ready=false"],
                    "blockers": [],
                },
            }
        },
    )
    monkeypatch.setattr(
        "data_platform.core.activation_preflight.build_go_live_report",
        lambda: {
            "environments": {
                "dev": {
                    "errors": [],
                    "warnings": [],
                    "go_live": {"blockers": []},
                },
            }
        },
    )

    result = run_activation_preflight()

    assert result["status"] == "WARN"
    assert result["environments"]["dev"]["status"] == "WARN"
