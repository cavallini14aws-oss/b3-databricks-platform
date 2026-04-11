from setuptools import find_packages, setup

setup(
    name="b3_data_platform",
    version="0.1.0",
    description="B3 Databricks Platform - Data Platform, ML and MLOps",
    author="Bruno Cavallini",
    packages=find_packages(include=["data_platform", "data_platform.*", "pipelines", "pipelines.*"]),
    include_package_data=True,
    install_requires=[],
)
