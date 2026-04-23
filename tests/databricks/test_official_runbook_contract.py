from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_official_runbook_exists():
    assert Path("docs/OFFICIAL_INTEGRATION_RUNBOOK.md").exists()

def test_official_runbook_has_required_sections():
    text = read("docs/OFFICIAL_INTEGRATION_RUNBOOK.md")
    assert "Pré-requisitos obrigatórios" in text
    assert "Dry-run official" in text
    assert "Deploy official" in text
    assert "Smoke pós-deploy" in text
    assert "Rollback" in text
    assert "Itens proibidos no pacote official" in text
