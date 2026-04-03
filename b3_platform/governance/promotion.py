from dataclasses import dataclass

from b3_platform.core.job_config import JobConfig


@dataclass(frozen=True)
class PromotionDecision:
    approved: bool
    reason: str


def evaluate_ml_promotion(
    job_config: JobConfig,
    accuracy: float | None,
    f1: float | None,
    auc: float | None,
    tests_passed: bool = True,
    manual_approval: bool = False,
) -> PromotionDecision:
    if job_config.promotion.require_tests_passed and not tests_passed:
        return PromotionDecision(
            approved=False,
            reason="Promotion blocked: automated tests did not pass.",
        )

    if job_config.promotion.requires_approval and not manual_approval:
        return PromotionDecision(
            approved=False,
            reason="Promotion blocked: manual approval is required.",
        )

    if job_config.promotion.require_quality_gates:
        gates = job_config.ml_quality_gates

        if accuracy is None or accuracy < gates.minimum_accuracy:
            return PromotionDecision(
                approved=False,
                reason=(
                    f"Promotion blocked: accuracy {accuracy} below "
                    f"minimum {gates.minimum_accuracy}."
                ),
            )

        if f1 is None or f1 < gates.minimum_f1:
            return PromotionDecision(
                approved=False,
                reason=(
                    f"Promotion blocked: f1 {f1} below minimum {gates.minimum_f1}."
                ),
            )

        if auc is None or auc < gates.minimum_auc:
            return PromotionDecision(
                approved=False,
                reason=(
                    f"Promotion blocked: auc {auc} below minimum {gates.minimum_auc}."
                ),
            )

    return PromotionDecision(
        approved=True,
        reason="Promotion approved.",
    )
