import json

from data_platform.orchestration.generate_platform_operational_report import (
    build_platform_operational_report,
)


def main() -> None:
    payload = build_platform_operational_report()
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
