from b3_platform.dataops.table_spec import ColumnSpec, TableSpec


TABLE_SPEC = TableSpec(
    catalog_template="clientes_{env}",
    schema_name="slv",
    table_name="tb_exemplo_template",
    columns=[
        ColumnSpec(
            name="id_registro",
            data_type="STRING",
            comment="Identificador único do registro",
        ),
        ColumnSpec(
            name="descricao",
            data_type="STRING",
            comment="Descrição textual do registro",
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
        "pii": "False",
        "dominio_negocio": "exemplo",
        "sensivel": "nao",
        "descricao_tabela": "Tabela de exemplo para template declarativo",
    },
    column_tags={
        "id_registro": {
            "pii": "não",
            "classificacao": "interno",
        },
        "descricao": {
            "pii": "não",
            "classificacao": "interno",
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
)
