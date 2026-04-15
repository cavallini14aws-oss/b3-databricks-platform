from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfile
from typing import Any



SEED_STATE_PATH = Path("data_platform/resources/state/release_state.json")
DEFAULT_RUNTIME_STATE_PATH = Path(".state/release_state.json")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _resolve_state_path() -> Path:
    custom_path = os.environ.get("RELEASE_STATE_PATH")
    if custom_path:
        return Path(custom_path)
    return DEFAULT_RUNTIME_STATE_PATH


def _ensure_runtime_state_exists() -> Path:
    runtime_path = _resolve_state_path()
    runtime_path.parent.mkdir(parents=True, exist_ok=True)

    if not runtime_path.exists():
        if not SEED_STATE_PATH.exists():
            raise FileNotFoundError(
                f"Seed state file nao encontrado: {SEED_STATE_PATH}"
            )
        copyfile(SEED_STATE_PATH, runtime_path)

    return runtime_path


def load_release_state() -> dict[str, Any]:
    state_path = _ensure_runtime_state_exists()
    return json.loads(state_path.read_text(encoding="utf-8"))


def save_release_state(state: dict[str, Any]) -> None:
    state_path = _ensure_runtime_state_exists()
    state_path.write_text(
        json.dumps(state, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def update_release_state(
    env: str,
    current_version: str | None = None,
    previous_version: str | None = None,
    smoke_status: str | None = None,
    rehearsal_status: str | None = None,
    rollback_candidate: str | None = None,
) -> dict[str, Any]:
    state = load_release_state()

    if env not in state:
        raise ValueError(f"Ambiente invalido para release state: {env}")

    if current_version is not None:
        state[env]["current_version"] = current_version

    if previous_version is not None:
        state[env]["previous_version"] = previous_version

    if current_version is not None or previous_version is not None:
        state[env]["last_promoted_at"] = utc_now_iso()

    if smoke_status is not None:
        state[env]["last_smoke_status"] = smoke_status

    if rehearsal_status is not None:
        state[env]["last_rehearsal_status"] = rehearsal_status

    if rollback_candidate is not None:
        state[env]["rollback_candidate"] = rollback_candidate

    save_release_state(state)
    return state


def update_smoke_status(env: str, status: str) -> dict[str, Any]:
    state = load_release_state()

    if env not in state:
        raise ValueError(f"Ambiente invalido para release state: {env}")

    state[env]["last_smoke_status"] = status
    save_release_state(state)
    return state


def update_rehearsal_status(env: str, status: str) -> dict[str, Any]:
    state = load_release_state()

    if env not in state:
        raise ValueError(f"Ambiente invalido para release state: {env}")

    state[env]["last_rehearsal_status"] = status
    save_release_state(state)
    return state

def set_smoke_status(env: str, status: str) -> dict[str, Any]:
    return update_smoke_status(env, status)


def set_rehearsal_status(env: str, status: str) -> dict[str, Any]:
    return update_rehearsal_status(env, status)

