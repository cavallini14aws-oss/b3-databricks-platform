"""Workload wrapper para rag_pdf_v1."""

def resolve_app():
    try:
        from services.api.rag_api import app as module
        return getattr(module, "app", module)
    except Exception:
        return None

app = resolve_app()

if __name__ == "__main__":
    raise SystemExit("rag_pdf_v1 deve ser executado pelo runtime/databricks job oficial.")
