import json
from datetime import datetime, timezone
from pathlib import Path


STATE_PATH = Path("data_platform/resources/state/release_state.json")


def load_release_state() -> dict:
    if not STATE_PATH.exists():
        raise FileNotFoundError(f"Release state não encontrado: {STATE_PATH}")
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_release_state(state: dict) -> None:
    STATE_PATH.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def set_env_release(
    env: str,
    current_version: str,
    previous_version: str | None,
    smoke_status: str | None = None,
    rehearsal_status: str | None = None,
) -> dict:
    state = load_release_state()
    state[env]["current_version"] = current_version
    state[env]["previous_version"] = previous_version
    state[env]["last_promoted_at"] = utc_now_iso()

    if smoke_status is not None:
        state[env]["last_smoke_status"] = smoke_status

    if rehearsal_status is not None and env == "hml":
        state[env]["last_rehearsal_status"] = rehearsal_status

    if env == "prd":
        state[env]["rollback_candidate"] = previous_version

    save_release_state(state)
    return state


def set_smoke_status(env: str, status: str) -> dict:
    state = load_release_state()
    state[env]["last_smoke_status"] = status
    save_release_state(state)
    return state


def set_rehearsal_status(env: str, status: str) -> dict:
    if env != "hml":
        raise ValueError("Rehearsal status atualmente só é suportado para hml")
    state = load_release_state()
    state[env]["last_rehearsal_status"] = status
    save_release_state(state)
    return state
