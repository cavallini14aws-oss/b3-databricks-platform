from pathlib import Path
import re


FORBIDDEN_PATTERNS = [
    r"Unicorn\.ia",
    r"/Users/brunocavallini/",
    r"pipelines\.examples\.pdf_rag_lab",
    r"pipelines/examples/pdf_rag_lab",
]


def iter_target_files():
    roots = [
        Path("databricks"),
        Path("docs/databricks"),
        Path("tests/databricks"),
    ]

    this_test = Path("tests/databricks/test_pdf_rag_isolation.py").resolve()

    for root in roots:
        if root.exists():
            for path in root.rglob("*"):
                if not path.is_file():
                    continue
                if "__pycache__" in path.parts:
                    continue
                if path.resolve() == this_test:
                    continue
                yield path

    bin_dir = Path("bin")
    if bin_dir.exists():
        for path in bin_dir.glob("dbx-*"):
            if path.is_file():
                yield path


def test_databricks_pdf_rag_isolation():
    violations = []

    for path in iter_target_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in FORBIDDEN_PATTERNS:
            if re.search(pattern, text):
                violations.append((str(path), pattern))

    assert not violations, f"Forbidden references found: {violations}"
