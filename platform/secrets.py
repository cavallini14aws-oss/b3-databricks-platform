from platform.context import PlatformContext


def get_secret_scope(context: PlatformContext) -> str:
    return context.secret_scope
