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
QUERY_TEXT = os.environ.get(
    "PDF_RAG_QUERY_TEXT",
    "Explique o significado psicológico do Livro Vermelho de Jung.",
)


def run_cmd(cmd: list[str]) -> dict:
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def submit_notebook() -> int:
    if not EXISTING_CLUSTER_ID:
        raise SystemExit("[ERROR] DBX_CLUSTER_ID não configurado")

    notebook = f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/04_query_vector_index"

    payload = {
        "run_name": "pdf-rag-vector-query-smoke",
        "existing_cluster_id": EXISTING_CLUSTER_ID,
        "notebook_task": {
            "notebook_path": notebook,
            "source": "WORKSPACE",
            "base_parameters": {
                "query_text": QUERY_TEXT,
            },
        },
    }

    response = run_cmd(
        [
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
    )
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
    run_id = submit_notebook()
    print(f"[OK] RUN_ID_VECTOR_QUERY_SMOKE={run_id}")
    wait_run(run_id)
    print("[OK] Vector query smoke completed successfully")


if __name__ == "__main__":
    main()
