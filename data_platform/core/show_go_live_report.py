import json

from data_platform.core.go_live_report import build_go_live_report


def main():
    report = build_go_live_report()
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
