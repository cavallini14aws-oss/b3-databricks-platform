from dataclasses import dataclass


@dataclass(frozen=True)
class PlatformFlags:
    enable_azure_devops: bool = True
    enable_github: bool = False
    enable_bitbucket: bool = False
    enable_aws: bool = False
    enable_databricks_native: bool = True
    enable_mlops: bool = True
    enable_llmops: bool = True
    enable_exploration: bool = True
    enable_automation: bool = True
