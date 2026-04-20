from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_hml_preflight_requires_m2m():
    text = read("bin/hml-preflight")
    assert "nao aceita DATABRICKS_CONFIG_PROFILE" in text
    assert "DATABRICKS_AUTH_TYPE=oauth-m2m" in text or "exige DATABRICKS_AUTH_TYPE=oauth-m2m" in text
    assert "DATABRICKS_CLIENT_ID" in text
    assert "DATABRICKS_CLIENT_SECRET" in text
    assert "check-runtime-env hml-m2m" in text

def test_prd_preflight_requires_m2m():
    text = read("bin/prd-preflight")
    assert "nao aceita DATABRICKS_CONFIG_PROFILE" in text
    assert "DATABRICKS_AUTH_TYPE=oauth-m2m" in text or "exige DATABRICKS_AUTH_TYPE=oauth-m2m" in text
    assert "DATABRICKS_CLIENT_ID" in text
    assert "DATABRICKS_CLIENT_SECRET" in text

def test_run_official_validation_guards_hml_prd():
    text = read("bin/run-official-validation")
    assert "nao aceita DATABRICKS_CONFIG_PROFILE" in text
    assert "DATABRICKS_AUTH_TYPE=oauth-m2m" in text or "exige DATABRICKS_AUTH_TYPE=oauth-m2m" in text
