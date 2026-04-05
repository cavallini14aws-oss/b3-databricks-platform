import argparse
from data_platform.flow_specs.generate_databricks_resources_yaml import build_resources_yaml

def main(args=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", required=True, choices=["dev", "hml", "prd"])
    parsed = parser.parse_args(args=args)
    print(build_resources_yaml(parsed.environment))

if __name__ == "__main__":
    main()
