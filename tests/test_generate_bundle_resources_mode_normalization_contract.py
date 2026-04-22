from data_platform.flow_specs.generate_bundle_resources import build_bundle_resources_payload


def test_existing_or_job_cluster_normalizes_to_classic(monkeypatch):
    monkeypatch.setattr(
        "data_platform.flow_specs.generate_bundle_resources.build_resources_payload",
        lambda environment: {
            "environment": environment,
            "resource_count": 1,
            "resources": [
                {
                    "resource_type": "job",
                    "job_name": f"ml_clientes_end_to_end_{environment}",
                    "environment": environment,
                    "project": "clientes",
                    "domain": "clientes",
                    "flow_name": "ml_clientes_end_to_end",
                    "flow_type": "ml",
                    "task_key": f"ml_clientes_end_to_end_{environment}_task",
                    "spec_module": "data_platform.flow_specs.projects.clientes.ml_clientes_end_to_end",
                    "entrypoint": "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_clientes_ml_end_to_end",
                    "callable_name": "run_clientes_ml_end_to_end",
                    "config_path": "config/clientes_ml_pipeline.yml",
                    "cluster_mode": "existing_or_job_cluster",
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
                "classic": {"spark_version": "14.3.x-scala2.12"},
                "serverless": {
                    "environment_key": "default",
                    "environment_version": "2",
                },
            },
            "targets": {},
        },
    )

    payload = build_bundle_resources_payload("dev")
    task = payload["resources"]["jobs"]["ml_clientes_end_to_end_dev"]["tasks"][0]

    assert task["cluster_mode"] == "classic"
    assert task["compute"]["requested_mode"] == "classic"
    assert task["compute"]["resolved_mode"] == "classic"


def test_serverless_mode_stays_serverless(monkeypatch):
    monkeypatch.setattr(
        "data_platform.flow_specs.generate_bundle_resources.build_resources_payload",
        lambda environment: {
            "environment": environment,
            "resource_count": 1,
            "resources": [
                {
                    "resource_type": "job",
                    "job_name": f"ml_clientes_end_to_end_{environment}",
                    "environment": environment,
                    "project": "clientes",
                    "domain": "clientes",
                    "flow_name": "ml_clientes_end_to_end",
                    "flow_type": "ml",
                    "task_key": f"ml_clientes_end_to_end_{environment}_task",
                    "spec_module": "data_platform.flow_specs.projects.clientes.ml_clientes_end_to_end",
                    "entrypoint": "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_clientes_ml_end_to_end",
                    "callable_name": "run_clientes_ml_end_to_end",
                    "config_path": "config/clientes_ml_pipeline.yml",
                    "cluster_mode": "serverless",
                    "use_catalog": True,
                    "tags": {"owner": "time_clientes"},
                }
            ],
        },
    )

    payload = build_bundle_resources_payload("prd")
    task = payload["resources"]["jobs"]["ml_clientes_end_to_end_prd"]["tasks"][0]

    assert task["cluster_mode"] == "serverless"
    assert task["compute"]["requested_mode"] == "serverless"
    assert task["compute"]["resolved_mode"] == "serverless"
