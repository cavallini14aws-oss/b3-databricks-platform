from dataclasses import dataclass


@dataclass(frozen=True)
class PlatformFlags:
    enable_data_pipeline: bool = True
    enable_ml_pipeline: bool = True
    enable_llm_pipeline: bool = False
    enable_baseline: bool = True
    enable_confusion_matrix: bool = True
    enable_predictions_logging: bool = True
    enable_registry: bool = True
    enable_feature_store: bool = False
    enable_strict_quality_gates: bool = False

    @classmethod
    def from_dict(cls, config: dict | None = None) -> "PlatformFlags":
        config = config or {}

        return cls(
            enable_data_pipeline=bool(config.get("enable_data_pipeline", True)),
            enable_ml_pipeline=bool(config.get("enable_ml_pipeline", True)),
            enable_llm_pipeline=bool(config.get("enable_llm_pipeline", False)),
            enable_baseline=bool(config.get("enable_baseline", True)),
            enable_confusion_matrix=bool(config.get("enable_confusion_matrix", True)),
            enable_predictions_logging=bool(config.get("enable_predictions_logging", True)),
            enable_registry=bool(config.get("enable_registry", True)),
            enable_feature_store=bool(config.get("enable_feature_store", False)),
            enable_strict_quality_gates=bool(config.get("enable_strict_quality_gates", False)),
        )

    @classmethod
    def default(cls) -> "PlatformFlags":
        return cls()
