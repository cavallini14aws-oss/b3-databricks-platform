import json

from data_platform.core.activation_readiness_report import (
    build_activation_readiness_report,
)


def main():
    report = build_activation_readiness_report()
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
