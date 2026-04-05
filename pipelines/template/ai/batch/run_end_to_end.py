from pipelines.template.ai.batch.ingest_knowledge import run_ingest_knowledge
from pipelines.template.ai.batch.chunk_documents import run_chunk_documents
from pipelines.template.ai.batch.build_embeddings import run_build_embeddings
from pipelines.template.ai.batch.build_index import run_build_index
from pipelines.template.ai.batch.evaluate_rag import run_evaluate_rag


def run_template_ai_batch_end_to_end(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    run_ingest_knowledge(spark=spark, project=project, use_catalog=use_catalog)
    run_chunk_documents(spark=spark, project=project, use_catalog=use_catalog)
    run_build_embeddings(spark=spark, project=project, use_catalog=use_catalog)
    run_build_index(spark=spark, project=project, use_catalog=use_catalog)
    run_evaluate_rag(spark=spark, project=project, use_catalog=use_catalog)
