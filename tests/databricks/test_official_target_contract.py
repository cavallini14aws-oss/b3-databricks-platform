from pathlib import Path
import yaml

def test_official_environment_contract_exists():
    path = Path("config/official_environment_contract.yml")
    assert path.exists()

def test_official_environment_contract_has_targets():
    data = yaml.safe_load(Path("config/official_environment_contract.yml").read_text(encoding="utf-8"))
    targets = data["targets"]

    assert targets["dev"]["mode"] == "development"
    assert targets["hml"]["mode"] == "production"
    assert targets["prd"]["mode"] == "production"

    assert "/Workspace/Users/" in targets["dev"]["workspace"]["root_path"]
    assert "/Workspace/Shared/" in targets["hml"]["workspace"]["root_path"]
    assert "/Workspace/Shared/" in targets["prd"]["workspace"]["root_path"]

    assert targets["dev"]["run_as"]["type"] == "user_name"
    assert targets["hml"]["run_as"]["type"] == "service_principal_name"
    assert targets["prd"]["run_as"]["type"] == "service_principal_name"

def test_generate_bundle_targets_reads_official_contract():
    text = Path("data_platform/flow_specs/generate_bundle_targets.py").read_text(encoding="utf-8")
    assert "official_environment_contract.yml" in text
    assert 'run_as"] = {"user_name": "${workspace.current_user.userName}"}' in text
    assert 'service_principal_name' in text
