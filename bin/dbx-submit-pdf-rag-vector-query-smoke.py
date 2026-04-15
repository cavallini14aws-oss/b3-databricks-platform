#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} nao definido")
    return value


DBX_PROFILE = require_env("DBX_PROFILE")
DBX_WS_ROOT = require_env("DBX_WS_ROOT")
QUERY_TEXT = os.environ.get(
    "PDF_RAG_QUERY_TEXT",
    "Explique o significado psicológico do Livro Vermelho de Jung.",
)


def cli_json(cmd: list[str]) -> dict:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    if not proc.stdout.strip():
        raise RuntimeError(f"Empty response: {' '.join(cmd)}")
    return json.loads(proc.stdout)


def submit_notebook_run(run_name: str, notebook_path: str) -> int:
    payload = {
        "run_name": run_name,
        "tasks": [
            {
                "task_key": "main",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "query_text": QUERY_TEXT,
                    },
                },
            }
        ],
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
    notebook = f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/04_query_vector_index"
    print(f"[INFO] Submitting vector query smoke notebook={notebook}")
    print(f"[INFO] query_text={QUERY_TEXT}")
    run_id = submit_notebook_run("pdf-rag-04-vector-query-smoke-submit", notebook)
    print(f"[OK] RUN_ID_VECTOR_QUERY_SMOKE={run_id}")
    wait_run(run_id)
    print("[OK] Vector query smoke completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
