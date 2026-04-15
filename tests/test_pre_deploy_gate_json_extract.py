import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path


def load_pre_deploy_gate_module():
    script_path = Path("bin/pre-deploy-gate")
    loader = SourceFileLoader("pre_deploy_gate", str(script_path))
    spec = importlib.util.spec_from_loader("pre_deploy_gate", loader)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_extract_json_from_output_handles_noise_before_json():
    module = load_pre_deploy_gate_module()
    raw = 'warning line\nanother line\n{"status": "ALLOW", "x": 1}'
    parsed = module.extract_json_from_output(raw)
    assert parsed["status"] == "ALLOW"
    assert parsed["x"] == 1
