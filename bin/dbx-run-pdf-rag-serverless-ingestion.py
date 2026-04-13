#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time


DBX_PROFILE = os.environ.get("DBX_PROFILE", "brunocavallini@hotmail.com")
DBX_WS_ROOT = os.environ.get("DBX_WS_ROOT", "/Workspace/Shared/pdf_rag")


def run_cli(cmd: list[str]) -> dict:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )

    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError(f"Empty JSON response from command: {' '.join(cmd)}")

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON from command: {' '.join(cmd)}\nSTDOUT:\n{stdout}\nSTDERR:\n{proc.stderr}"
        ) from exc


def jobs_list() -> list[dict]:
    data = run_cli(
        ["databricks", "jobs", "list", "--profile", DBX_PROFILE, "-o", "json"]
    )
    return data if isinstance(data, list) else data.get("jobs", [])


def ensure_job(job_name: str, notebook_path: str) -> int:
    for job in jobs_list():
        settings = job.get("settings", {})
        if settings.get("name") == job_name:
            return int(job["job_id"])

    payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "main",
                "notebook_task": {
                    "notebook_path": notebook_path
                }
            }
        ]
    }

    data = run_cli(
        [
            "databricks",
            "jobs",
            "create",
            "--profile",
            DBX_PROFILE,
            "--json",
            json.dumps(payload),
            "-o",
            "json",
        ]
    )
    return int(data["job_id"])


def run_job(job_id: int) -> int:
    data = run_cli(
        [
            "databricks",
            "jobs",
            "run-now",
            str(job_id),
            "--profile",
            DBX_PROFILE,
            "-o",
            "json",
        ]
    )
    return int(data["run_id"])


def get_run(run_id: int) -> dict:
    return run_cli(
        [
            "databricks",
            "jobs",
            "get-run",
            str(run_id),
            "--profile",
            DBX_PROFILE,
            "-o",
            "json",
        ]
    )


def wait_run(run_id: int) -> None:
    while True:
        data = get_run(run_id)
        state = data.get("state", {})
        life_cycle = state.get("life_cycle_state", "")
        result_state = state.get("result_state", "")
        state_message = state.get("state_message", "")

        print(
            f"[INFO] run_id={run_id} life_cycle_state={life_cycle} "
            f"result_state={result_state} message={state_message}"
        )

        if life_cycle == "TERMINATED":
            if result_state == "SUCCESS":
                return
            raise RuntimeError(json.dumps(data, ensure_ascii=False, indent=2))

        if life_cycle in {"INTERNAL_ERROR", "SKIPPED"}:
            raise RuntimeError(json.dumps(data, ensure_ascii=False, indent=2))

        time.sleep(10)


def main() -> None:
    notebook_01 = f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/01_inspect_documents"
    notebook_02 = f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/02_ingest_pages_and_chunks"

    print("[INFO] Ensuring job 01")
    job_id_01 = ensure_job("pdf-rag-01-inspect-documents", notebook_01)
    print(f"[OK] JOB_ID_01={job_id_01}")

    print("[INFO] Ensuring job 02")
    job_id_02 = ensure_job("pdf-rag-02-ingest-pages-and-chunks", notebook_02)
    print(f"[OK] JOB_ID_02={job_id_02}")

    print("[INFO] Running job 01")
    run_id_01 = run_job(job_id_01)
    print(f"[OK] RUN_ID_01={run_id_01}")
    wait_run(run_id_01)

    print("[INFO] Running job 02")
    run_id_02 = run_job(job_id_02)
    print(f"[OK] RUN_ID_02={run_id_02}")
    wait_run(run_id_02)

    print("[OK] Both notebook jobs completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
