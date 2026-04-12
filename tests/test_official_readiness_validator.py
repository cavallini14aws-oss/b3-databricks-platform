from data_platform.governance.official_readiness_validator import validate_official_readiness


def test_validate_official_readiness_allow(tmp_path):
    cfg = tmp_path / "official.yml"
    cfg.write_text(
        """
environment: dev
storage:
  mode: table
  catalog: demo
  schema: silver
  volume_catalog: demo
  volume_schema: ai
  volume_name: pdf_rag_lab_docs
  volume_path: /Volumes/demo/ai/pdf_rag_lab_docs
grants:
  required_table_permissions:
    - USE CATALOG
  required_volume_permissions:
    - READ VOLUME
identity:
  allowed_executor_types:
    - user
  expected_group: zsbr_dev_ai_ops
compute:
  allowed_cluster_modes:
    - job_cluster
  min_runtime: "15.4"
  requires_uc_access: true
naming:
  expected_catalog: demo
  expected_schema: silver
  expected_volume_schema: ai
""",
        encoding="utf-8",
    )

    result = validate_official_readiness(str(cfg))
    assert result["valid"] is True


def test_validate_official_readiness_block(tmp_path):
    cfg = tmp_path / "broken.yml"
    cfg.write_text("environment: dev\n", encoding="utf-8")

    result = validate_official_readiness(str(cfg))
    assert result["valid"] is False
