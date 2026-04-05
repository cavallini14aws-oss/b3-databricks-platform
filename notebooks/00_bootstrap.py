from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger

ctx = get_context(project="clientes", use_catalog=False)
logger = PlatformLogger(
    component="00_bootstrap",
    env=ctx.env,
    project=ctx.project,
)

logger.info("Bootstrap iniciado")
logger.info(f"env={ctx.env}")
logger.info(f"catalog={ctx.naming.catalog}")
logger.info(f"schema_bronze={ctx.naming.schema_bronze}")
logger.info(f"schema_silver={ctx.naming.schema_silver}")
logger.info(f"schema_gold={ctx.naming.schema_gold}")
logger.info(f"schema_obs={ctx.naming.schema_obs}")
logger.info(f"secret_scope={ctx.secret_scope}")
logger.info(f"enable_azure_devops={ctx.flags.enable_azure_devops}")
logger.info(f"enable_databricks_native={ctx.flags.enable_databricks_native}")
logger.info("Bootstrap finalizado")
