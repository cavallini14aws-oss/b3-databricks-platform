from pathlib import Path

TARGETS = [
    "bin/render-databricks-runtime-spec",
    "bin/render-databricks-job-libraries",
    "bin/render-databricks-job-spec",
    "bin/render-databricks-bundle-fragment",
    "bin/render-official-bundle-target",
    "bin/deploy-official-profile",
    "bin/apply-official-profile",
]

def test_profile_scripts_marked_internal():
    for target in TARGETS:
        text = Path(target).read_text(encoding="utf-8")
        assert "INTERFACE INTERNA" in text
        assert "--workload" in text or "interface recomendada" in text
