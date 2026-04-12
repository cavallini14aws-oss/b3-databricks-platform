# FREE EDITION ONLY
# Dependências devem ser providas pelo runtime oficial da plataforma em ambientes dev/hml/prd.
# Não usar instalação manual de dependências como padrão operacional em ambientes oficiais.

# Databricks notebook source
wheel_path = "/Workspace/Shared/.bundle/databricks-platform-template/dev/artifacts/.internal/databricks_platform_template-0.1.0-py3-none-any.whl"

# COMMAND ----------

# MAGIC %pip install $wheel_path

# COMMAND ----------

from data_platform.mlops.run_mlops_drift_cycle import main

main()
