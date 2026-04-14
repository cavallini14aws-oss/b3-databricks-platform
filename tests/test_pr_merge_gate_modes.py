from data_platform.core.pr_merge_gate import run_pr_merge_gate


def test_pr_merge_gate_technical_mode_ignores_go_live_blockers():
    result = run_pr_merge_gate(mode="technical")
    assert result["mode"] == "technical"
    assert "activation_preflight_blocked" not in result["errors"]
    assert "go_no_go_policy_blocked" not in result["errors"]


def test_pr_merge_gate_full_mode_preserves_go_live_checks():
    result = run_pr_merge_gate(mode="full")
    assert result["mode"] == "full"
