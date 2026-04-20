#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

DBX_PROFILE = os.environ.get("DBX_PROFILE", "DEFAULT_DATABRICKS_PROFILE")
DBX_WS_ROOT = os.environ.get("DBX_WS_ROOT", "/Workspace/Shared/pdf_rag")


def cli_json(cmd: list[str]) -> dict:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    if not proc.stdout.strip():
        raise RuntimeError(f"Empty response: {' '.join(cmd)}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        ) from exc


def submit_notebook_run(run_name: str, notebook_path: str) -> int:
    payload = {
        "run_name": run_name,
        "tasks": [
            {
                "task_key": "main",
                "notebook_task": {
                    "notebook_path": notebook_path
                }
            }
        ]
    }

    data = cli_json(
        [
            "databricks",
            "api",
            "post",
            "/api/2.2/jobs/runs/submit",
            "--profile",
            DBX_PROFILE,
            "--json",
            json.dumps(payload),
        ]
    )
    return int(data["run_id"])


def get_run(run_id: int) -> dict:
    return cli_json(
        [
            "databricks",
            "api",
            "get",
            f"/api/2.2/jobs/runs/get?run_id={run_id}",
            "--profile",
            DBX_PROFILE,
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
            f"[INFO] run_id={run_id} "
            f"life_cycle_state={life_cycle} "
            f"result_state={result_state} "
            f"message={state_message}"
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

    print("[INFO] Submitting notebook 01")
    run_id_01 = submit_notebook_run("pdf-rag-01-inspect-documents-submit", notebook_01)
    print(f"[OK] RUN_ID_01={run_id_01}")
    wait_run(run_id_01)

    print("[INFO] Submitting notebook 02")
    run_id_02 = submit_notebook_run("pdf-rag-02-ingest-pages-and-chunks-submit", notebook_02)
    print(f"[OK] RUN_ID_02={run_id_02}")
    wait_run(run_id_02)

    print("[OK] Both notebook runs completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
