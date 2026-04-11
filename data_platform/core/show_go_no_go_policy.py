import json

from data_platform.core.go_no_go_policy import evaluate_go_no_go


def main():
    result = evaluate_go_no_go()
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
