from pathlib import Path


def test_environment_activation_checklists_exist():
    assert Path("docs/checklists/mlops_exp_activation_checklist.md").exists()
    assert Path("docs/checklists/mlops_dev_activation_checklist.md").exists()
    assert Path("docs/checklists/mlops_hml_activation_checklist.md").exists()
    assert Path("docs/checklists/mlops_prd_activation_checklist.md").exists()
