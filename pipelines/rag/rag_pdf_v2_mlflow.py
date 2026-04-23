"""Workload wrapper para rag_pdf_v2_mlflow."""

def resolve_app():
    try:
        from services.api.ml_serving import app as module
        return getattr(module, "app", module)
    except Exception:
        return None

app = resolve_app()

if __name__ == "__main__":
    raise SystemExit("rag_pdf_v2_mlflow deve ser executado pelo runtime/databricks job oficial.")
