from datetime import datetime
from uuid import uuid4


class PlatformLogger:
    def __init__(self, component: str, env: str, project: str, run_id: str | None = None):
        self.component = component
        self.env = env
        self.project = project
        self.run_id = run_id or str(uuid4())
        self.started_at = datetime.utcnow()

    def _emit(self, level: str, message: str) -> None:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"{timestamp} | level={level.upper()} | env={self.env} | "
            f"project={self.project} | component={self.component} | "
            f"run_id={self.run_id} | msg={message}",
            flush=True,
        )

    def info(self, message: str) -> None:
        self._emit("info", message)

    def warn(self, message: str) -> None:
        self._emit("warn", message)

    def error(self, message: str) -> None:
        self._emit("error", message)