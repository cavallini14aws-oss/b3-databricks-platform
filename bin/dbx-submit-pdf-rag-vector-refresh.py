#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time


DBX_PROFILE = os.environ.get("DBX_PROFILE", "brunocavallini@hotmail.com")
DBX_WS_ROOT = os.environ.get("DBX_WS_ROOT", "/Workspace/Shared/pdf_rag")
EXISTING_CLUSTER_ID = os.environ.get("DBX_CLUSTER_ID")


def run_cmd(cmd: list[str]) -> dict:
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def submit_notebook(notebook_path: str) -> int:
    if not EXISTING_CLUSTER_ID:
        raise SystemExit("[ERROR] DBX_CLUSTER_ID não configurado")

    payload = {
        "run_name": "pdf-rag-vector-refresh",
        "existing_cluster_id": EXISTING_CLUSTER_ID,
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "WORKSPACE",
        },
    }

    cmd = [
        "databricks",
        "jobs",
        "submit",
        "--json",
        json.dumps(payload),
        "--profile",
        DBX_PROFILE,
        "-o",
        "json",
    ]

    response = run_cmd(cmd)
    return int(response["run_id"])


def wait_run(run_id: int) -> None:
    while True:
        response = run_cmd(
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

        state = response.get("state", {})
        life_cycle_state = state.get("life_cycle_state", "")
        result_state = state.get("result_state", "")
        message = state.get("state_message", "")

        print(
            f"[INFO] run_id={run_id} life_cycle_state={life_cycle_state} "
            f"result_state={result_state} message={message}",
            flush=True,
        )

        if life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            if result_state != "SUCCESS":
                raise SystemExit(f"[ERROR] run failed: {json.dumps(state, ensure_ascii=False)}")
            return

        time.sleep(10)


def main() -> None:
    notebook = f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/03_create_vector_index"
    print(f"[INFO] submitting vector refresh notebook={notebook}")
    run_id = submit_notebook(notebook)
    print(f"[OK] RUN_ID_VECTOR_REFRESH={run_id}")
    wait_run(run_id)
    print("[OK] Vector refresh notebook completed successfully")


if __name__ == "__main__":
    main()
