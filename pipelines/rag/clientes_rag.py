"""Workload wrapper para clientes_rag."""

def resolve_app():
    try:
        from services.api.rag_api import app as module
        return getattr(module, "app", module)
    except Exception:
        return None

app = resolve_app()

if __name__ == "__main__":
    raise SystemExit("Entrypoint RAG resolvido como wrapper. Execute via runtime oficial.")
