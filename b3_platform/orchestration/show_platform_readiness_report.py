import json

from b3_platform.orchestration.generate_platform_readiness_report import (
    build_platform_readiness_report,
)


def main() -> None:
    payload = build_platform_readiness_report()
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
