from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger

ctx = get_context(project="clientes", use_catalog=False)
logger = PlatformLogger(
    component="01_setup_demo_tables",
    env=ctx.env,
    project=ctx.project,
)

logger.info("Criando schemas iniciais")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_gold}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.schema_obs}")

logger.info("Schemas criados com sucesso")
