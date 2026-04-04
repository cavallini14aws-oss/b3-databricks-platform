import json

from b3_platform.flow_specs.generate_registry import build_registry_payload


def main() -> None:
    payload = build_registry_payload()
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
