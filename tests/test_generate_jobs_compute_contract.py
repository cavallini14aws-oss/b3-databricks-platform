from data_platform.flow_specs.generate_jobs import build_jobs_payload


def test_generate_jobs_preserves_cluster_key_and_use_catalog():
    payload = build_jobs_payload("dev")
    assert payload["environment"] == "dev"
    assert payload["job_count"] >= 1

    job = payload["jobs"][0]
    assert "cluster_key" in job
    assert isinstance(job["cluster_key"], str)
    assert "use_catalog" in job
    assert isinstance(job["use_catalog"], bool)


def test_generate_jobs_can_carry_cluster_mode_metadata(monkeypatch):
    monkeypatch.setattr(
        "data_platform.flow_specs.generate_jobs.build_registry_payload",
        lambda: {
            "flows": [
                {
                    "enabled": True,
                    "project": "clientes",
                    "domain": "clientes",
                    "flow_name": "ml_clientes_end_to_end",
                    "flow_type": "ml",
                    "spec_module": "data_platform.flow_specs.projects.clientes.ml_clientes_end_to_end",
                    "entrypoint": "pipelines.examples.clientes.ml.run_clientes_ml_end_to_end.run_clientes_ml_end_to_end",
                    "callable_name": "run_clientes_ml_end_to_end",
                    "tags": {"owner": "time_clientes"},
                }
            ]
        },
    )

    class FakeCfg:
        cluster_key = "clientes-hml-cluster"
        cluster_mode = "serverless"
        use_catalog = True
        default_config_path = "config/clientes_ml_pipeline.yml"

    monkeypatch.setattr(
        "data_platform.flow_specs.generate_jobs.load_job_config",
        lambda env: FakeCfg(),
    )

    payload = build_jobs_payload("hml")
    job = payload["jobs"][0]

    assert job["cluster_key"] == "clientes-hml-cluster"
    assert job["cluster_mode"] == "serverless"
    assert job["use_catalog"] is True
