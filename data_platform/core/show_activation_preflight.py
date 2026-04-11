import json

from data_platform.core.activation_preflight import run_activation_preflight


def main():
    result = run_activation_preflight()
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
