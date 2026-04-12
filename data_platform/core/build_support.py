import glob
import subprocess
import sys
from pathlib import Path


def ensure_project_root_on_path() -> Path:
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    return project_root


def ensure_wheel_exists() -> str:
    wheels = sorted(glob.glob("dist/*.whl"))
    if wheels:
        return wheels[-1]

    build_cmd = [sys.executable, "-m", "build", "--wheel"]
    result = subprocess.run(build_cmd, text=True, capture_output=True)

    if result.stdout:
        print(result.stdout, flush=True)
    if result.stderr:
        print(result.stderr, flush=True)

    if result.returncode != 0:
        raise SystemExit(result.returncode)

    wheels = sorted(glob.glob("dist/*.whl"))
    if not wheels:
        print("[BLOCK] wheel não encontrada mesmo após build")
        raise SystemExit(1)

    return wheels[-1]
