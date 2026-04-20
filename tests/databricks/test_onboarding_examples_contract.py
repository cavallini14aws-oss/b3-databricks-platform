from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def test_onboarding_doc_exists():
    assert Path("docs/ONBOARDING_PLATFORM_EXAMPLES.md").exists()

def test_onboarding_doc_maps_examples():
    text = read("docs/ONBOARDING_PLATFORM_EXAMPLES.md")
    assert "example_data_bronze" in text
    assert "example_data_silver" in text
    assert "example_data_gold" in text
    assert "example_rag_standard" in text
    assert "example_rag_mlflow" in text
    assert "example_ml_training" in text
    assert "example_ml_inference" in text

def test_examples_readme_points_to_onboarding():
    text = read("examples/databricks/README.md")
    assert "ONBOARDING_PLATFORM_EXAMPLES.md" in text
