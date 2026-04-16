#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from typing import Any


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} nao definido")
    return value


DBX_PROFILE = require_env("DBX_PROFILE")
DBX_WS_ROOT = require_env("DBX_WS_ROOT")
QUESTION = os.environ.get(
    "PDF_RAG_QUESTION",
    "Explique o significado psicológico do Livro Vermelho de Jung.",
)
FOUNDATION_ENDPOINT = os.environ.get(
    "DATABRICKS_FOUNDATION_ENDPOINT",
    "databricks-gpt-5-4-mini",
)


def cli_json(cmd: list[str]) -> dict[str, Any]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )

    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError(f"Empty response: {' '.join(cmd)}")

    return json.loads(stdout)


def submit_notebook_run() -> int:
    notebook = f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/05_rag_answer_ptbr"

    payload = {
        "run_name": "pdf-rag-05-answer-ptbr-smoke-submit",
        "tasks": [
            {
                "task_key": "main",
                "notebook_task": {
                    "notebook_path": notebook,
                    "base_parameters": {"question": QUESTION, "foundation_endpoint": FOUNDATION_ENDPOINT},
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


def get_run(run_id: int) -> dict[str, Any]:
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


def get_run_output(run_id: int) -> dict[str, Any]:
    return cli_json(
        [
            "databricks",
            "jobs",
            "get-run-output",
            str(run_id),
            "--profile",
            DBX_PROFILE,
            "-o",
            "json",
        ]
    )


def extract_failed_task_run_id(run_data: dict[str, Any]) -> int | None:
    tasks = run_data.get("tasks", [])

    failed_tasks = [
        task
        for task in tasks
        if task.get("state", {}).get("result_state") not in {None, "", "SUCCESS"}
    ]

    if failed_tasks:
        return int(failed_tasks[-1]["run_id"])

    if tasks:
        return int(tasks[-1]["run_id"])

    return None


def print_task_output(task_run_id: int) -> None:
    print(f"[INFO] fetching task output task_run_id={task_run_id}")
    output = get_run_output(task_run_id)
    print("[INFO] task output:")
    print(json.dumps(output, ensure_ascii=False, indent=2))


def wait_run(run_id: int) -> None:
    while True:
        data = get_run(run_id)
        state = data.get("state", {})
        life_cycle = state.get("life_cycle_state", "")
        result_state = state.get("result_state", "")
        message = state.get("state_message", "")

        print(
            f"[INFO] run_id={run_id} "
            f"life_cycle_state={life_cycle} "
            f"result_state={result_state} "
            f"message={message}"
        )

        if life_cycle == "TERMINATED":
            if result_state == "SUCCESS":
                return

            task_run_id = extract_failed_task_run_id(data)
            if task_run_id:
                print_task_output(task_run_id)

            raise RuntimeError(json.dumps(data, ensure_ascii=False, indent=2))

        if life_cycle in {"INTERNAL_ERROR", "SKIPPED"}:
            task_run_id = extract_failed_task_run_id(data)
            if task_run_id:
                print_task_output(task_run_id)

            raise RuntimeError(json.dumps(data, ensure_ascii=False, indent=2))

        time.sleep(10)


def main() -> None:
    print("[INFO] submitting RAG answer PT-BR smoke")
    print(f"[INFO] question={QUESTION}")
    print(f"[INFO] foundation_endpoint={FOUNDATION_ENDPOINT}")

    run_id = submit_notebook_run()
    print(f"[OK] RUN_ID_RAG_ANSWER_SMOKE={run_id}")

    wait_run(run_id)
    print("[OK] RAG answer PT-BR smoke completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
