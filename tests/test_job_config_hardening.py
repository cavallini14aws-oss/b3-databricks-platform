from data_platform.core.job_config import load_job_config


def test_hml_and_prd_quality_gates_are_hardened(monkeypatch):
    def fake_loader(path):
        if path.endswith("hml.yml"):
            return {
                "job": {"environment": "hml"},
                "quality_gates": {
                    "ml": {
                        "minimum_accuracy": 0.65,
                        "minimum_f1": 0.55,
                        "minimum_auc": 0.65,
                    }
                },
            }
        if path.endswith("prd.yml"):
            return {
                "job": {"environment": "prd"},
                "quality_gates": {
                    "ml": {
                        "minimum_accuracy": 0.75,
                        "minimum_f1": 0.70,
                        "minimum_auc": 0.75,
                    }
                },
            }
        return {"job": {"environment": "dev"}}

    monkeypatch.setattr(
        "data_platform.core.job_config.load_yaml_config",
        fake_loader,
    )

    hml = load_job_config("hml")
    prd = load_job_config("prd")

    assert hml.ml_quality_gates.minimum_accuracy == 0.65
    assert hml.ml_quality_gates.minimum_f1 == 0.55
    assert hml.ml_quality_gates.minimum_auc == 0.65

    assert prd.ml_quality_gates.minimum_accuracy == 0.75
    assert prd.ml_quality_gates.minimum_f1 == 0.70
    assert prd.ml_quality_gates.minimum_auc == 0.75
