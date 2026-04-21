from pathlib import Path
import json

from data_platform.flow_specs.generate_bundle_resources import build_bundle_resources_payload


def test_generated_bundle_resources_dev_contains_jobs():
    payload = build_bundle_resources_payload("dev")

    assert payload["environment"] == "dev"
    assert "resources" in payload
    assert "jobs" in payload["resources"]
    assert len(payload["resources"]["jobs"]) > 0


def test_generated_bundle_resources_hml_contains_jobs():
    payload = build_bundle_resources_payload("hml")

    assert payload["environment"] == "hml"
    assert "resources" in payload
    assert "jobs" in payload["resources"]
    assert len(payload["resources"]["jobs"]) > 0


def test_generated_bundle_resources_prd_contains_jobs():
    payload = build_bundle_resources_payload("prd")

    assert payload["environment"] == "prd"
    assert "resources" in payload
    assert "jobs" in payload["resources"]
    assert len(payload["resources"]["jobs"]) > 0
