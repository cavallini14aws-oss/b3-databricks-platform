"""Workload wrapper para clientes_api."""

def resolve_app():
    try:
        from services.api.rag_api import app as module
        return getattr(module, "app", module)
    except Exception:
        return None

app = resolve_app()

if __name__ == "__main__":
    raise SystemExit("Entrypoint API resolvido como wrapper. Execute via runtime oficial.")
