import argparse
import json

from b3_platform.core.job_config import load_job_config
from b3_platform.governance.promotion import evaluate_ml_promotion, log_promotion_decision


def _str_to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value

    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False

    raise ValueError(f"Valor booleano inválido: {value}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Avalia promoção de modelo ML entre ambientes."
    )
    parser.add_argument("--source-env", required=True)
    parser.add_argument("--target-env", required=True)
    parser.add_argument("--project", default="clientes")
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--accuracy", type=float, default=None)
    parser.add_argument("--f1", type=float, default=None)
    parser.add_argument("--auc", type=float, default=None)
    parser.add_argument("--tests-passed", default="true")
    parser.add_argument("--manual-approval", default="false")
    parser.add_argument("--use-catalog", default="false")
    parser.add_argument("--run-id", default="promotion-cli")
    parser.add_argument(
        "--persist-decision",
        default="false",
        help="true/false. Se true e houver spark disponível, grava a decisão em tabela técnica.",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    tests_passed = _str_to_bool(parsed.tests_passed)
    manual_approval = _str_to_bool(parsed.manual_approval)
    use_catalog = _str_to_bool(parsed.use_catalog)
    persist_decision = _str_to_bool(parsed.persist_decision)

    target_job_config = load_job_config(parsed.target_env)

    decision = evaluate_ml_promotion(
        job_config=target_job_config,
        accuracy=parsed.accuracy,
        f1=parsed.f1,
        auc=parsed.auc,
        tests_passed=tests_passed,
        manual_approval=manual_approval,
    )

    result = {
        "source_env": parsed.source_env,
        "target_env": parsed.target_env,
        "project": parsed.project,
        "model_name": parsed.model_name,
        "model_version": parsed.model_version,
        "approved": decision.approved,
        "reason": decision.reason,
        "accuracy": parsed.accuracy,
        "f1": parsed.f1,
        "auc": parsed.auc,
        "tests_passed": tests_passed,
        "manual_approval": manual_approval,
        "persist_decision": persist_decision,
    }

    if persist_decision:
        try:
            spark  # type: ignore[name-defined]
        except NameError as exc:
            raise RuntimeError(
                "Spark session não encontrada. Para persistir a decisão em tabela técnica, "
                "execute este CLI em ambiente Databricks com variável global 'spark'."
            ) from exc

        log_promotion_decision(
            spark=spark,  # type: ignore[name-defined]
            model_name=parsed.model_name,
            model_version=parsed.model_version,
            decision=decision,
            run_id=parsed.run_id,
            source_env=parsed.source_env,
            target_env=parsed.target_env,
            accuracy=parsed.accuracy,
            f1=parsed.f1,
            auc=parsed.auc,
            tests_passed=tests_passed,
            manual_approval=manual_approval,
            project=parsed.project,
            use_catalog=use_catalog,
        )

    print(json.dumps(result, indent=2))

    if not decision.approved:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
