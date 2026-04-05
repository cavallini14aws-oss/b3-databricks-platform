import json

from data_platform.flow_specs.discovery import discover_flow_specs


def main() -> None:
    payload = {
        "flow_specs": discover_flow_specs(),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
