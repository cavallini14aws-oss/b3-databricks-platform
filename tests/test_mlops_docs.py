from pathlib import Path


def test_ml_degradation_docs_exist():
    assert Path("docs/runbooks/ml_degradation_runbook.md").exists()
    assert Path("docs/checklists/ml_degradation_checklist.md").exists()
