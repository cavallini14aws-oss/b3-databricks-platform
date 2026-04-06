from data_platform.mlops.scoring_runs import SCORING_RUN_SCHEMA, log_scoring_run


class FakeWriter:
    def __init__(self):
        self.mode_value = None
        self.options = {}
        self.saved_table = None

    def mode(self, value):
        self.mode_value = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def saveAsTable(self, table_name):
        self.saved_table = table_name
        return None


class FakeDataFrame:
    def __init__(self):
        self.write = FakeWriter()


class FakeSpark:
    def __init__(self, table_exists):
        self._table_exists = table_exists
        self.created_schema = None
        self.created_df = None

        class Catalog:
            def __init__(self, exists):
                self._exists = exists

            def tableExists(self, name):
                return self._exists

        self.catalog = Catalog(table_exists)

    def sql(self, statement):
        self.created_schema = statement

    def createDataFrame(self, rows, schema=None):
        self.created_df = FakeDataFrame()
        return self.created_df


def test_scoring_run_schema_has_expected_fields():
    field_names = [field.name for field in SCORING_RUN_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "target_env",
        "input_table",
        "output_table",
        "input_count",
        "prediction_count",
        "artifact_path",
        "run_id",
    ]


def test_log_scoring_run_creates_table_when_missing():
    spark = FakeSpark(table_exists=False)

    log_scoring_run(
        spark=spark,
        model_name="clientes_status_classifier",
        model_version="v1",
        target_env="hml",
        input_table="clientes_feature.tb_clientes_scoring_dataset_v2",
        output_table="clientes_mlops.tb_clientes_status_predictions_hml",
        input_count=6,
        prediction_count=6,
        artifact_path="/tmp/model",
        run_id="run-1",
        project="clientes",
        use_catalog=False,
    )

    assert spark.created_schema is not None
    assert spark.created_df is not None
    assert spark.created_df.write.mode_value == "overwrite"
    assert spark.created_df.write.options["overwriteSchema"] == "true"
    assert spark.created_df.write.saved_table == "clientes_mlops.tb_model_scoring_runs"


def test_log_scoring_run_appends_when_table_exists():
    spark = FakeSpark(table_exists=True)

    log_scoring_run(
        spark=spark,
        model_name="clientes_status_classifier",
        model_version="v1",
        target_env="prd",
        input_table="clientes_feature.tb_clientes_scoring_dataset_v2",
        output_table="clientes_mlops.tb_clientes_status_predictions_prd",
        input_count=6,
        prediction_count=6,
        artifact_path="/tmp/model",
        run_id="run-2",
        project="clientes",
        use_catalog=False,
    )

    assert spark.created_df is not None
    assert spark.created_df.write.mode_value == "append"
    assert spark.created_df.write.saved_table == "clientes_mlops.tb_model_scoring_runs"
