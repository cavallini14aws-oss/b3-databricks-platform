from setuptools import find_packages, setup

setup(
    name="databricks_platform_template",
    version="0.1.0",
    description="Generic Databricks platform template for data pipelines, MLOps and LLMOps",
    author="Bruno Cavallini",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.10",
    packages=find_packages(
        include=[
            "data_platform",
            "data_platform.*",
            "pipelines",
            "pipelines.*",
        ]
    ),
    include_package_data=True,
    package_data={
        "data_platform.resources": ["**/*.yml", "**/*.yaml"],
    },
    install_requires=[
        "PyYAML==6.0.3",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "mlflow==3.10.1",
            "pyspark==4.1.1",
        ]
    },
    entry_points={
        "console_scripts": [
            "run_mlops_drift_cycle=data_platform.mlops.run_mlops_drift_cycle:main",
            "run_mlops_postprod_cycle=data_platform.mlops.run_mlops_postprod_cycle:main",
            "run_mlops_retraining_cycle=data_platform.mlops.run_mlops_retraining_cycle:main",
            "run_mlops_operational_cycle=data_platform.mlops.run_mlops_operational_cycle:main",
        ]
    },
)
