from data_platform.core.context import PlatformContext


def get_secret_scope(context: PlatformContext) -> str:
    return context.secret_scope


def resolve_secret(
    *,
    context: PlatformContext,
    key: str,
    dbutils=None,
) -> str:
    if not key:
        raise ValueError("Chave de secret vazia")

    scope = get_secret_scope(context)

    if dbutils is None:
        raise RuntimeError(
            "dbutils não informado para resolução de secret em runtime Databricks"
        )

    try:
        value = dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:
        raise RuntimeError(
            f"Falha ao resolver secret no scope={scope} para key={key}"
        ) from exc

    if not value:
        raise RuntimeError(
            f"Secret vazio resolvido no scope={scope} para key={key}"
        )

    return value


def build_databricks_secrets_resolver(
    *,
    context: PlatformContext,
    dbutils,
):
    def _resolver(scope: str, key: str) -> str:
        if not scope:
            raise ValueError("Scope de secret vazio")
        if not key:
            raise ValueError("Key de secret vazia")

        try:
            value = dbutils.secrets.get(scope=scope, key=key)
        except Exception as exc:
            raise RuntimeError(
                f"Falha ao resolver secret no scope={scope} para key={key}"
            ) from exc

        if not value:
            raise RuntimeError(
                f"Secret vazio resolvido no scope={scope} para key={key}"
            )

        return value

    return _resolver
