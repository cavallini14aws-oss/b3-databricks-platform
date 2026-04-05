import argparse
import json
from data_platform.flow_specs.generate_bundle_resources import build_bundle_resources_payload

def main(args=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", required=True, choices=["dev", "hml", "prd"])
    parsed = parser.parse_args(args=args)
    print(json.dumps(build_bundle_resources_payload(parsed.environment), indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
