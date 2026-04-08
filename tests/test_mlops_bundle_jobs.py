from pathlib import Path


def test_mlops_bundle_jobs_file_exists():
    assert Path("resources/jobs/mlops_operational_jobs.yml").exists()
