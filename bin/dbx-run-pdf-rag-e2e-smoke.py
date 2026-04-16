#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


REQUIRED_ENV_VARS = [
    "DBX_PROFILE",
    "DBX_WS_ROOT",
]


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} nao definido")
    return value


def run_cmd(cmd: list[str]) -> None:
    print(f"[CMD] {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, text=True)

    if proc.returncode != 0:
        raise RuntimeError(f"Command failed with code={proc.returncode}: {' '.join(cmd)}")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_evidence(status: str, started_at: str, ended_at: str, error: str | None = None) -> None:
    output_dir = Path("artifacts/pdf_rag")
    output_dir.mkdir(parents=True, exist_ok=True)

    evidence = {
        "component": "pdf_rag_e2e_smoke",
        "status": status,
        "started_at": started_at,
        "ended_at": ended_at,
        "profile": os.environ.get("DBX_PROFILE"),
        "workspace_root": os.environ.get("DBX_WS_ROOT"),
        "local_env": os.environ.get("LOCAL_ENV", "dev"),
        "foundation_endpoint_override": os.environ.get("DATABRICKS_FOUNDATION_ENDPOINT"),
        "steps": [
            "workspace_import",
            "vector_refresh",
            "vector_query_smoke",
            "rag_answer_ptbr_smoke",
        ],
        "error": error,
    }

    path = output_dir / "pdf_rag_e2e_smoke_evidence.json"
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[OK] evidence written: {path}")


def main() -> None:
    started_at = utc_now()

    try:
        for env_var in REQUIRED_ENV_VARS:
            require_env(env_var)

        os.environ.setdefault("LOCAL_ENV", "dev")

        dbx_profile = os.environ["DBX_PROFILE"]
        dbx_ws_root = os.environ["DBX_WS_ROOT"]

        print("[INFO] Starting PDF RAG E2E smoke")
        print(f"[INFO] LOCAL_ENV={os.environ.get('LOCAL_ENV')}")
        print(f"[INFO] DBX_WS_ROOT={dbx_ws_root}")
        print(f"[INFO] DATABRICKS_FOUNDATION_ENDPOINT={os.environ.get('DATABRICKS_FOUNDATION_ENDPOINT', '')}")

        run_cmd(
            [
                "databricks",
                "workspace",
                "import-dir",
                "databricks",
                f"{dbx_ws_root}/databricks",
                "--overwrite",
                "--profile",
                dbx_profile,
            ]
        )

        run_cmd(["./bin/dbx-submit-pdf-rag-vector-refresh.py"])
        run_cmd(["./bin/dbx-submit-pdf-rag-vector-query-smoke.py"])
        run_cmd(["./bin/dbx-submit-pdf-rag-answer-smoke.py"])

        ended_at = utc_now()
        write_evidence(status="SUCCESS", started_at=started_at, ended_at=ended_at)
        print("[OK] PDF RAG E2E smoke completed successfully")

    except Exception as exc:
        ended_at = utc_now()
        write_evidence(
            status="FAILED",
            started_at=started_at,
            ended_at=ended_at,
            error=str(exc),
        )
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
