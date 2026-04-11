import json

from data_platform.core.pipeline_registry_validation import (
    validate_pipeline_registry_artifacts,
)


def main():
    result = validate_pipeline_registry_artifacts()
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
