from pathlib import Path
import re

TARGET_DIRS = [
    Path("bin"),
    Path("databricks"),
    Path("config"),
]

ALLOWLIST = {
    "requirements-runtime.txt",
    "requirements-dev.txt",
    "requirements-dev-min.txt",
    "requirements-platform.txt",
}

PATTERNS = [
    re.compile(r'^\s*%pip\s+install(\s|$)', re.MULTILINE),
    re.compile(r'^\s*!pip\s+install(\s|$)', re.MULTILINE),
    re.compile(r'^\s*pip\s+install(\s|$)', re.MULTILINE),
    re.compile(r'os\.system\(\s*["\']pip install', re.MULTILINE),
    re.compile(r'subprocess\.(run|Popen)\(.*pip.*install', re.MULTILINE | re.DOTALL),
]

def test_no_manual_pip_install_in_official_paths():
    offenders = []

    for target in TARGET_DIRS:
        if not target.exists():
            continue

        for path in target.rglob("*"):
            if not path.is_file():
                continue

            rel = path.as_posix()
            if rel in ALLOWLIST:
                continue

            try:
                text = path.read_text(encoding="utf-8")
            except Exception:
                continue

            for pattern in PATTERNS:
                if pattern.search(text):
                    offenders.append(rel)
                    break

    assert not offenders, "Encontrado pip install manual em caminhos oficiais: " + ", ".join(sorted(set(offenders)))
