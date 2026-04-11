import json

from data_platform.core.pr_merge_gate import run_pr_merge_gate


def main():
    result = run_pr_merge_gate()
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if result["decision"] == "BLOCK":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
