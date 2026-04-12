from data_platform.governance.runtime_install_validator import scan_manual_install_patterns


def test_runtime_install_validator_dev_is_valid():
    result = scan_manual_install_patterns("dev")
    assert result["valid"] is True
    assert result["environment"] == "dev"


def test_runtime_install_validator_hml_returns_expected_shape():
    result = scan_manual_install_patterns("hml")
    assert "valid" in result
    assert "matches" in result
    assert "approved_matches" in result
    assert result["environment"] == "hml"
