from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_check_governance_assets_exists():
    text = read("bin/check-governance-assets")
    assert "create_table.sql" in text
    assert "apply_tags.py" in text
    assert "masking_policy_example.py" in text
    assert "descricao_tabela" in text

def test_check_governance_assets_checks_column_tags():
    text = read("bin/check-governance-assets")
    assert "pii" in text
    assert "classificacao" in text
    assert "mascaramento" in text

def test_examples_directory_exists():
    assert Path("examples/databricks").exists()
