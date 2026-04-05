from data_platform.dataops.table_spec import ColumnSpec, TableSpec


TABLE_SPEC = TableSpec(
    catalog_name=None,
    schema_name="clientes_silver",
    table_name="tb_cliente_exemplo",
    table_description="Tabela de exemplo real do domínio clientes",
    owner=None,
    columns=[
        ColumnSpec(
            name="id_cliente",
            data_type="STRING",
            comment="Identificador único do cliente",
        ),
        ColumnSpec(
            name="nome_cliente",
            data_type="STRING",
            comment="Nome principal do cliente",
        ),
        ColumnSpec(
            name="ingestion_date",
            data_type="TIMESTAMP",
            comment="Data de ingestão dos dados",
        ),
        ColumnSpec(
            name="update_date",
            data_type="TIMESTAMP",
            comment="Última atualização dos dados",
        ),
    ],
    table_properties={
        "delta.enableDeletionVectors": "true",
        "delta.feature.appendOnly": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.invariants": "supported",
        "delta.minReaderVersion": "3",
        "delta.minWriterVersion": "7",
    },
    table_tags={
        "periodicidade": "diario",
        "sla": "N/A",
        "data_owner": "N/A",
        "data_steward": "N/A",
        "tempo_retencao_dados": "N/A",
        "particionada": "False",
        "classificacao_seguranca": "Interno",
        "pii": "True",
        "dominio_negocio": "clientes",
        "sensivel": "sim",
        "descricao_tabela": "Tabela de exemplo real do domínio clientes",
    },
    column_tags={
        "id_cliente": {
            "pii": "não",
            "classificacao": "interno",
        },
        "nome_cliente": {
            "pii": "sim",
            "classificacao": "confidencial",
        },
        "ingestion_date": {
            "pii": "não",
            "classificacao": "interno",
        },
        "update_date": {
            "pii": "não",
            "classificacao": "interno",
        },
    },
    using_format="delta",
    create_catalog_if_not_exists=False,
    create_schema_if_not_exists=True,
)
