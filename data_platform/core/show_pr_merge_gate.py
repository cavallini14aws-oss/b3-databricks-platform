import json
import os

from data_platform.core.pr_merge_gate import run_pr_merge_gate


def main():
    mode = os.environ.get("PR_GATE_MODE", "full")
    result = run_pr_merge_gate(mode=mode)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if result["decision"] == "BLOCK":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
