from data_platform.flow_specs.generate_bundle_resources import build_bundle_resources_payload


def test_dynamic_environment_uses_target_config_when_present(monkeypatch):
    monkeypatch.setattr(
        "data_platform.flow_specs.generate_bundle_resources.build_resources_payload",
        lambda environment: {
            "environment": environment,
            "resource_count": 1,
            "resources": [
                {
                    "resource_type": "job",
                    "job_name": f"job_{environment}",
                    "environment": environment,
                    "project": "clientes",
                    "domain": "clientes",
                    "flow_name": "job",
                    "flow_type": "ml",
                    "task_key": f"task_{environment}",
                    "spec_module": "x",
                    "entrypoint": "y",
                    "callable_name": "z",
                    "config_path": "config/x.yml",
                    "cluster_mode": None,
                    "use_catalog": True,
                    "tags": {"owner": "time_clientes"},
                }
            ],
        },
    )

    monkeypatch.setattr(
        "data_platform.flow_specs.generate_bundle_resources._load_compute_matrix",
        lambda: {
            "version": 1,
            "defaults": {
                "mode": "auto",
                "classic": {
                    "spark_version": "15.4.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "driver_node_type_id": "i3.xlarge",
                    "num_workers": 0,
                },
                "serverless": {
                    "environment_key": "default",
                    "environment_version": "2",
                },
            },
            "targets": {
                "qa": {
                    "mode": "serverless",
                }
            },
        },
    )

    payload = build_bundle_resources_payload("qa")
    task = payload["resources"]["jobs"]["job_qa"]["tasks"][0]

    assert payload["environment"] == "qa"
    assert task["compute"]["requested_mode"] == "serverless"
    assert task["compute"]["resolved_mode"] == "serverless"


def test_generic_mode_normalizes_to_classic(monkeypatch):
    monkeypatch.setattr(
        "data_platform.flow_specs.generate_bundle_resources.build_resources_payload",
        lambda environment: {
            "environment": environment,
            "resource_count": 1,
            "resources": [
                {
                    "resource_type": "job",
                    "job_name": f"job_{environment}",
                    "environment": environment,
                    "project": "clientes",
                    "domain": "clientes",
                    "flow_name": "job",
                    "flow_type": "ml",
                    "task_key": f"task_{environment}",
                    "spec_module": "x",
                    "entrypoint": "y",
                    "callable_name": "z",
                    "config_path": "config/x.yml",
                    "cluster_mode": "generic",
                    "use_catalog": True,
                    "tags": {"owner": "time_clientes"},
                }
            ],
        },
    )

    payload = build_bundle_resources_payload("sandbox")
    task = payload["resources"]["jobs"]["job_sandbox"]["tasks"][0]

    assert task["cluster_mode"] == "classic"
    assert task["compute"]["requested_mode"] == "classic"
    assert task["compute"]["resolved_mode"] == "classic"
