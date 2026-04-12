from data_platform.table_specs.base.base_table_spec import BaseColumnSpec, BaseTableSpec


PDF_RAG_DOCUMENTS_SPEC = BaseTableSpec(
    catalog="<catalog>",
    schema="<schema>",
    table_name="tb_pdf_rag_documents",
    table_description="Tabela de documentos ingeridos pelo PDF RAG Lab.",
    table_tags={
        "domain": "ai",
        "layer": "silver",
        "contains_pii": "false",
        "project_type": "rag_lab",
    },
    columns=[
        BaseColumnSpec("document_id", "STRING", comment="Identificador único do documento."),
        BaseColumnSpec("file_name", "STRING", comment="Nome do arquivo PDF original."),
        BaseColumnSpec("source_path", "STRING", comment="Caminho de origem do arquivo."),
        BaseColumnSpec("document_title", "STRING", comment="Título identificado ou derivado do documento."),
        BaseColumnSpec("ingestion_ts", "TIMESTAMP", comment="Data e hora de ingestão do documento."),
        BaseColumnSpec("project", "STRING", comment="Projeto associado ao documento."),
    ],
)


PDF_RAG_CHUNKS_SPEC = BaseTableSpec(
    catalog="<catalog>",
    schema="<schema>",
    table_name="tb_pdf_rag_chunks",
    table_description="Tabela de chunks textuais gerados a partir dos documentos do PDF RAG Lab.",
    table_tags={
        "domain": "ai",
        "layer": "silver",
        "contains_pii": "false",
        "project_type": "rag_lab",
    },
    columns=[
        BaseColumnSpec("document_id", "STRING", comment="Identificador do documento de origem."),
        BaseColumnSpec("chunk_id", "STRING", comment="Identificador único do chunk."),
        BaseColumnSpec("chunk_index", "INT", comment="Posição sequencial do chunk no documento."),
        BaseColumnSpec("page_number", "INT", comment="Página do PDF de onde o chunk foi extraído."),
        BaseColumnSpec("chunk_text", "STRING", comment="Texto do chunk recuperável."),
        BaseColumnSpec("project", "STRING", comment="Projeto associado ao chunk."),
    ],
)


PDF_RAG_EMBEDDINGS_SPEC = BaseTableSpec(
    catalog="<catalog>",
    schema="<schema>",
    table_name="tb_pdf_rag_embeddings",
    table_description="Tabela de embeddings gerados para os chunks do PDF RAG Lab.",
    table_tags={
        "domain": "ai",
        "layer": "silver",
        "contains_pii": "false",
        "project_type": "rag_lab",
    },
    columns=[
        BaseColumnSpec("document_id", "STRING", comment="Identificador do documento de origem."),
        BaseColumnSpec("chunk_id", "STRING", comment="Identificador do chunk vetorizado."),
        BaseColumnSpec("chunk_index", "INT", comment="Posição sequencial do chunk."),
        BaseColumnSpec("embedding", "ARRAY<FLOAT>", comment="Vetor numérico do chunk."),
        BaseColumnSpec("embedding_model", "STRING", comment="Modelo utilizado para gerar o embedding."),
        BaseColumnSpec("project", "STRING", comment="Projeto associado ao embedding."),
    ],
)


PDF_RAG_QUERIES_SPEC = BaseTableSpec(
    catalog="<catalog>",
    schema="<schema>",
    table_name="tb_pdf_rag_queries",
    table_description="Tabela de consultas e respostas do PDF RAG Lab para rastreabilidade e análise.",
    table_tags={
        "domain": "ai",
        "layer": "silver",
        "contains_pii": "false",
        "project_type": "rag_lab",
    },
    columns=[
        BaseColumnSpec("query_id", "STRING", comment="Identificador único da consulta."),
        BaseColumnSpec("question_text", "STRING", comment="Pergunta enviada pelo usuário."),
        BaseColumnSpec("retrieved_chunk_count", "INT", comment="Quantidade de chunks recuperados."),
        BaseColumnSpec("response_text", "STRING", comment="Resposta final gerada pelo LLM."),
        BaseColumnSpec("llm_model", "STRING", comment="Modelo LLM utilizado na resposta."),
        BaseColumnSpec("created_ts", "TIMESTAMP", comment="Data e hora da consulta."),
        BaseColumnSpec("project", "STRING", comment="Projeto associado à consulta."),
    ],
)
