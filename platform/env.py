import os


ALLOWED_ENVS = {"dev", "hml", "prd"}


def get_env(default: str = "dev") -> str:
    env = os.getenv("LOCAL_ENV", default).strip().lower()

    if env not in ALLOWED_ENVS:
        raise ValueError(
            f"LOCAL_ENV inválido: {env}. Valores permitidos: {sorted(ALLOWED_ENVS)}"
        )

    return env
