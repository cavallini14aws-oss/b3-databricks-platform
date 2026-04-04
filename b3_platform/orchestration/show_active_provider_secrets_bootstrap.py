from b3_platform.orchestration.ci_provider_config import get_active_ci_provider
from b3_platform.orchestration.generate_active_provider_secrets_bootstrap import (
    build_env_example,
    build_markdown,
)


def main() -> None:
    active_provider = get_active_ci_provider()

    print("===== ENV EXAMPLE =====")
    print(build_env_example(active_provider.name))
    print("===== MARKDOWN =====")
    print(build_markdown(active_provider.name))


if __name__ == "__main__":
    main()
