from data_platform.flow_specs.generate_resources import build_resources_payload


def test_generate_resources_preserves_use_catalog():
    payload = build_resources_payload("dev")
    assert payload["environment"] == "dev"
    assert payload["resource_count"] >= 1

    resource = payload["resources"][0]
    assert "use_catalog" in resource
    assert isinstance(resource["use_catalog"], bool)


def test_generate_resources_can_carry_cluster_mode(monkeypatch):
    monkeypatch.setattr(
        "data_platform.flow_specs.generate_resources.build_jobs_payload",
        lambda environment: {
            "environment": environment,
            "job_count": 1,
            "jobs": [
                {
                    "job_name": f"ml_clientes_end_to_end_{environment}",
                    "environment": environment,
                    "project": "clientes",
                    "domain": "clientes",
                    "flow_name": "ml_clientes_end_to_end",
                    "flow_type": "ml",
                    "spec_module": "data_platform.flow_specs.projects.clientes.ml_clientes_end_to_end",
                    "entrypoint": "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_clientes_ml_end_to_end",
                    "callable_name": "run_clientes_ml_end_to_end",
                    "cluster_key": f"clientes-{environment}-cluster",
                    "cluster_mode": "serverless",
                    "use_catalog": True,
                    "config_path": "config/clientes_ml_pipeline.yml",
                    "tags": {"owner": "time_clientes"},
                }
            ],
        },
    )

    payload = build_resources_payload("hml")
    resource = payload["resources"][0]

    assert resource["job_name"] == "ml_clientes_end_to_end_hml"
    assert resource["cluster_mode"] == "serverless"
    assert resource["use_catalog"] is True
