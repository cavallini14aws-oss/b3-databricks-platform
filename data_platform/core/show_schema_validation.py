import json

from data_platform.core.schema_validation import validate_required_schema_specs


def main():
    result = validate_required_schema_specs()
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
