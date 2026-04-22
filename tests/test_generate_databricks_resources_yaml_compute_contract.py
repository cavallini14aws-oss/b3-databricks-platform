from data_platform.flow_specs.generate_databricks_resources_yaml import build_resources_yaml


def test_rendered_resources_yaml_contains_job_name():
    rendered = build_resources_yaml("dev")
    assert "ml_clientes_end_to_end_dev" in rendered
    assert "resources:" in rendered
    assert "jobs:" in rendered


def test_rendered_resources_yaml_can_reflect_cluster_mode_metadata(monkeypatch):
    monkeypatch.setattr(
        "data_platform.flow_specs.generate_databricks_resources_yaml.build_bundle_resources_payload",
        lambda environment: {
            "resources": {
                "jobs": {
                    f"ml_clientes_end_to_end_{environment}": {
                        "name": f"ml_clientes_end_to_end_{environment}",
                        "tags": {"owner": "time_clientes"},
                        "tasks": [
                            {
                                "task_key": f"ml_clientes_end_to_end_{environment}_task",
                                "cluster_mode": "serverless",
                                "compute": {
                                    "resolved_mode": "serverless",
                                    "serverless": {
                                        "environment_key": "default",
                                        "environment_version": "2",
                                        "dependencies": [],
                                    },
                                },
                                "spark_python_task": {
                                    "python_file": "${workspace.root_path}/data_platform/flow_specs/run_flow_by_path.py",
                                    "parameters": [
                                        "--spec-module",
                                        "data_platform.flow_specs.projects.clientes.ml_clientes_end_to_end",
                                        "--project",
                                        "clientes",
                                        "--use-catalog",
                                        "true",
                                        "--config-path",
                                        "config/clientes_ml_pipeline.yml",
                                    ],
                                },
                            }
                        ],
                    }
                }
            }
        },
    )

    rendered = build_resources_yaml("hml")

    assert 'description: "cluster_mode=serverless"' in rendered
    assert "ml_clientes_end_to_end_hml" in rendered
