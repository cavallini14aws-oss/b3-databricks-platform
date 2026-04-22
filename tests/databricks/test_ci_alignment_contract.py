from pathlib import Path


FILES_MUST_NOT_CONTAIN_CLUSTER_ID = [
    ".github/workflows/databricks-ci.yml",
    ".github/workflows/deploy-dev.yml",
    ".github/workflows/deploy-hml.yml",
    ".github/workflows/deploy-prd.yml",
    ".github/workflows/pr-gate.yml",
    "azure-pipelines.yml",
    "bitbucket-pipelines.yml",
    "apply_ci_hardening.sh",
    "ci_adapters/github_actions/github-actions.yml",
    "ci_adapters/github_actions/CONTRACT.md",
    "ci_adapters/github_actions/README.md",
    "ci_adapters/aws/CONTRACT.md",
    "ci_adapters/azure_devops/CONTRACT.md",
    "ci_adapters/bitbucket/CONTRACT.md",
    "ci_adapters/azure_devops/azure-pipelines.yml",
    "ci_adapters/bitbucket/bitbucket-pipelines.yml",
    "ci_adapters/aws/buildspec.yml",
]

FILES_MUST_CONTAIN_DEPLOY_USER = [
    ".github/workflows/databricks-ci.yml",
    "azure-pipelines.yml",
    "bitbucket-pipelines.yml",
    "ci_adapters/github_actions/github-actions.yml",
    "ci_adapters/github_actions/CONTRACT.md",
    "ci_adapters/github_actions/README.md",
    "ci_adapters/aws/CONTRACT.md",
    "ci_adapters/azure_devops/CONTRACT.md",
    "ci_adapters/bitbucket/CONTRACT.md",
    "ci_adapters/azure_devops/azure-pipelines.yml",
    "ci_adapters/bitbucket/bitbucket-pipelines.yml",
    "ci_adapters/aws/buildspec.yml",
    "config/ci_secrets_contract.yml",
    "data_platform/resources/config/ci_secrets_contract.yml",
]

FILES_MUST_USE_OFFICIAL_CLI_INSTALL = [
    ".github/workflows/databricks-ci.yml",
    ".github/workflows/deploy-dev.yml",
    ".github/workflows/deploy-hml.yml",
    ".github/workflows/deploy-prd.yml",
    ".github/workflows/pr-gate.yml",
    "azure-pipelines.yml",
    "bitbucket-pipelines.yml",
    "ci_adapters/github_actions/github-actions.yml",
    "ci_adapters/azure_devops/azure-pipelines.yml",
    "ci_adapters/bitbucket/bitbucket-pipelines.yml",
    "ci_adapters/aws/buildspec.yml",
    "data_platform/orchestration/generate_ci_adapters.py",
]

FILES_MUST_PASS_DEPLOY_USER_VAR = [
    ".github/workflows/databricks-ci.yml",
    "azure-pipelines.yml",
    "bitbucket-pipelines.yml",
    "ci_adapters/github_actions/github-actions.yml",
    "ci_adapters/azure_devops/azure-pipelines.yml",
    "ci_adapters/bitbucket/bitbucket-pipelines.yml",
    "ci_adapters/aws/buildspec.yml",
    "data_platform/orchestration/generate_ci_adapters.py",
]

PROMOTION_WRAPPER_WORKFLOWS = [
    ".github/workflows/deploy-dev.yml",
    ".github/workflows/deploy-hml.yml",
    ".github/workflows/deploy-prd.yml",
]

PR_GATE_WORKFLOWS = [
    ".github/workflows/pr-gate.yml",
]


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_no_cluster_id_residue_in_ci_files():
    for path in FILES_MUST_NOT_CONTAIN_CLUSTER_ID:
        assert "CLUSTER_ID" not in read(path), path


def test_deploy_user_present_in_ci_contracts_and_workflows():
    for path in FILES_MUST_CONTAIN_DEPLOY_USER:
        assert "DEPLOY_USER" in read(path), path


def test_official_cli_install_used_in_ci_files():
    for path in FILES_MUST_USE_OFFICIAL_CLI_INSTALL:
        content = read(path)
        assert "setup-cli/main/install.sh" in content, path
        assert "pip install databricks-cli" not in content, path


def test_deploy_user_var_propagated_to_bundle_commands():
    for path in FILES_MUST_PASS_DEPLOY_USER_VAR:
        assert '--var="deploy_user=' in read(path), path


def test_promotion_wrapper_workflows_delegate_to_guarded_flow():
    for path in PROMOTION_WRAPPER_WORKFLOWS:
        content = read(path)
        assert "promote-guarded" in content or "dry-run-official-deploy" in content, path
        assert "CLUSTER_ID" not in content, path


def test_pr_gate_workflows_use_official_cli_without_cluster_id():
    for path in PR_GATE_WORKFLOWS:
        content = read(path)
        assert "setup-cli/main/install.sh" in content, path
        assert "pip install databricks-cli" not in content, path
        assert "CLUSTER_ID" not in content, path


def test_bundle_pins_databricks_cli_version():
    content = read("databricks.yml")
    assert "databricks_cli_version:" in content
    assert ">= 0.275.0, < 1.0.0" in content
