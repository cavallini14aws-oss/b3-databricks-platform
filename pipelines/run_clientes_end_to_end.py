from io import StringIO

import pandas as pd

from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger
from b3_platform.pipeline_runner import run_with_observability
from b3_platform.config_loader import load_yaml_config
from pipelines.ingest_file_clientes import run_ingest_file_clientes
from pipelines.ingest_table_clientes import run_ingest_table_clientes
from pipelines.silver_consolidado_clientes import run_silver_consolidado_clientes
from pipelines.gold_clientes_ativos import run_gold_clientes_ativos
from pipelines.gold_clientes_survivorship import run_gold_clientes_survivorship


def run_clientes_end_to_end(
    spark,
    config_path: str,
) -> None:
    config = load_yaml_config(config_path)

    project = config["project"]
    use_catalog = config["use_catalog"]

    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="run_clientes_end_to_end",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = base_logger.run_id

    def _run(logger: PlatformLogger):
        logger.info(f"Config YAML carregada: {config_path}")

        steps = config["steps"]

        if steps.get("ingest_file"):
            logger.info("Executando ingestão por arquivo")

            csv_content = config["sources"]["file"]["csv_content"]
            source_path = config["sources"]["file"]["source_path"]

            pdf = pd.read_csv(StringIO(csv_content))
            df_input = spark.createDataFrame(pdf)

            run_ingest_file_clientes(
                spark=spark,
                df_input=df_input,
                source_path=source_path,
                project=project,
                use_catalog=use_catalog,
            )

        if steps.get("ingest_table"):
            logger.info("Executando ingestão por tabela")
            run_ingest_table_clientes(
                spark=spark,
                project=project,
                use_catalog=use_catalog,
            )

        if steps.get("silver_consolidado"):
            logger.info("Executando silver consolidado")
            run_silver_consolidado_clientes(
                spark=spark,
                project=project,
                use_catalog=use_catalog,
            )

        if steps.get("gold_ativos"):
            logger.info("Executando gold ativos")
            run_gold_clientes_ativos(
                spark=spark,
                project=project,
                use_catalog=use_catalog,
            )

        if steps.get("gold_survivorship"):
            logger.info("Executando gold survivorship")
            run_gold_clientes_survivorship(
                spark=spark,
                project=project,
                use_catalog=use_catalog,
            )

        logger.info("Pipeline end-to-end concluído com sucesso")

    run_with_observability(
        spark=spark,
        component="run_clientes_end_to_end",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
    )