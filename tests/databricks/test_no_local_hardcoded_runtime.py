from pathlib import Path

PART_1 = "Uni"
PART_2 = "corn"
PART_3 = ".ia"

MAIL_A = "brunocavallini"
MAIL_B = "hotmail"
MAIL_C = "com"

PATTERNS = [
    PART_1 + PART_2 + PART_3,
    MAIL_A + "@" + MAIL_B + "." + MAIL_C,
]

TARGETS = [
    Path("bin"),
    Path("config"),
    Path("data_platform"),
    Path("tests"),
]

ALLOWLIST = {
    "tests/databricks/test_no_local_hardcoded_runtime.py",
}

def test_no_local_hardcoded_runtime():
    offenders = []

    for target in TARGETS:
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
                if pattern in text:
                    offenders.append(f"{path}: {pattern}")

    assert not offenders, "Hardcoded local encontrado:\\n" + "\\n".join(offenders)
