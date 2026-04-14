#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from typing import Any


DBX_PROFILE = os.environ.get("DBX_PROFILE", "brunocavallini@hotmail.com")
DBX_WS_ROOT = os.environ.get("DBX_WS_ROOT", "/Workspace/Shared/pdf_rag")


def run_cmd(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print("[ERROR] command failed:")
        print(" ".join(cmd))
        if result.stdout:
            print("[STDOUT]")
            print(result.stdout)
        if result.stderr:
            print("[STDERR]")
            print(result.stderr)
        raise SystemExit(result.returncode)
    return result.stdout.strip()


def api_get(path: str) -> dict[str, Any]:
    out = run_cmd(["databricks", "api", "get", path, "--profile", DBX_PROFILE])
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        print("[ERROR] failed to parse JSON from api_get")
        print(out)
        raise


def api_post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    out = run_cmd([
        "databricks", "api", "post", path,
        "--profile", DBX_PROFILE,
        "--json", json.dumps(payload),
    ])
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        print("[ERROR] failed to parse JSON from api_post")
        print(out)
        raise


def get_job_id_by_name(job_name: str) -> int | None:
    data = api_get("/api/2.2/jobs/list")
    for item in data.get("jobs", []):
        settings = item.get("settings", {})
        if settings.get("name") == job_name:
            return item["job_id"]
    return None


def ensure_job(job_name: str, notebook_path: str) -> int:
    existing = get_job_id_by_name(job_name)
    if existing:
        print(f"[OK] job already exists: {job_name} -> {existing}")
        return existing

    payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": f"task_{job_name.replace('-', '_')}",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 3600,
            }
        ],
    }

    print(f"[INFO] creating job: {job_name}")
    data = api_post("/api/2.2/jobs/create", payload)
    job_id = data["job_id"]
    print(f"[OK] created job: {job_name} -> {job_id}")
    return job_id


def run_job(job_id: int) -> int:
    data = api_post("/api/2.2/jobs/run-now", {"job_id": job_id})
    run_id = data["run_id"]
    print(f"[OK] started run_id={run_id} for job_id={job_id}")
    return run_id


def wait_run(run_id: int) -> None:
    print(f"[INFO] waiting run_id={run_id}")
    while True:
        data = api_get(f"/api/2.2/jobs/runs/get?run_id={run_id}")
        state = data.get("state", {})
        life_cycle = state.get("life_cycle_state", "UNKNOWN")
        result_state = state.get("result_state", "")
        state_msg = state.get("state_message", "")

        print(
            f"[TIMELINE] run_id={run_id} "
            f"life_cycle={life_cycle} "
            f"result_state={result_state} "
            f"message={state_msg}"
        )

        if life_cycle == "TERMINATED" and result_state == "SUCCESS":
            print(f"[OK] run_id={run_id} finished successfully")
            return

        if life_cycle in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED", "BLOCKED"} and result_state != "SUCCESS":
            print("[ERROR] run failed")
            print(json.dumps(data, ensure_ascii=False, indent=2))
            raise SystemExit(1)

        time.sleep(10)


def main() -> None:
    print(f"[INFO] profile={DBX_PROFILE}")
    print(f"[INFO] workspace_root={DBX_WS_ROOT}")

    job_01 = ensure_job(
        "pdf-rag-01-inspect-documents",
        f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/01_inspect_documents",
    )
    job_02 = ensure_job(
        "pdf-rag-02-ingest-pages-and-chunks",
        f"{DBX_WS_ROOT}/databricks/pdf_rag/notebooks/02_ingest_pages_and_chunks",
    )

    run_01 = run_job(job_01)
    wait_run(run_01)

    run_02 = run_job(job_02)
    wait_run(run_02)

    print("[OK] both notebook jobs completed successfully")


if __name__ == "__main__":
    main()
