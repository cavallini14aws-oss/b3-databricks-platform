from datetime import datetime


class PlatformLogger:
    def __init__(self, component: str, env: str, project: str):
        self.component = component
        self.env = env
        self.project = project

    def _emit(self, level: str, message: str) -> None:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"{timestamp} | level={level.upper()} | env={self.env} | "
            f"project={self.project} | component={self.component} | msg={message}",
            flush=True,
        )

    def info(self, message: str) -> None:
        self._emit("info", message)

    def warn(self, message: str) -> None:
        self._emit("warn", message)

    def error(self, message: str) -> None:
        self._emit("error", message)
