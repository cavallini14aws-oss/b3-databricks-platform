from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger

ctx = get_context(project="clientes")
logger = PlatformLogger(
    component="01_setup_demo_tables",
    env=ctx.env,
    project=ctx.project,
)

logger.info("Criando schemas iniciais")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {ctx.naming.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.catalog}.{ctx.naming.schema_bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.catalog}.{ctx.naming.schema_silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.catalog}.{ctx.naming.schema_gold}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.naming.catalog}.{ctx.naming.schema_obs}")

logger.info("Schemas criados com sucesso")
