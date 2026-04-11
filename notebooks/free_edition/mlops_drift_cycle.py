# Databricks notebook source
wheel_path = "/Workspace/Shared/.bundle/databricks-platform-template/dev/artifacts/.internal/databricks_platform_template-0.1.0-py3-none-any.whl"

# COMMAND ----------

# MAGIC %pip install $wheel_path

# COMMAND ----------

from data_platform.mlops.run_mlops_drift_cycle import main

main()
