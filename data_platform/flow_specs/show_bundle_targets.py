import json
from data_platform.flow_specs.generate_bundle_targets import build_bundle_targets_payload

def main() -> None:
    print(json.dumps(build_bundle_targets_payload(), indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
