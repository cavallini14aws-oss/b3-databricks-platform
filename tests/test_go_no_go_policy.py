from data_platform.core.go_no_go_policy import evaluate_go_no_go


def test_evaluate_go_no_go_returns_go(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.go_no_go_policy.run_activation_preflight",
        lambda: {
            "status": "PASS",
            "environments": {},
        },
    )

    result = evaluate_go_no_go()
    assert result["decision"] == "GO"


def test_evaluate_go_no_go_returns_go_with_risk(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.go_no_go_policy.run_activation_preflight",
        lambda: {
            "status": "WARN",
            "environments": {},
        },
    )

    result = evaluate_go_no_go()
    assert result["decision"] == "GO_WITH_RISK"


def test_evaluate_go_no_go_returns_no_go(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.go_no_go_policy.run_activation_preflight",
        lambda: {
            "status": "BLOCK",
            "environments": {},
        },
    )

    result = evaluate_go_no_go()
    assert result["decision"] == "NO_GO"
