from dataclasses import dataclass


@dataclass(frozen=True)
class Naming:
    env: str
    project: str
    use_catalog: bool = False

    @property
    def catalog(self) -> str:
        return f"b3_{self.env}"

    @property
    def schema_bronze(self) -> str:
        return f"{self.project}_bronze"

    @property
    def schema_silver(self) -> str:
        return f"{self.project}_silver"

    @property
    def schema_gold(self) -> str:
        return f"{self.project}_gold"

    @property
    def schema_obs(self) -> str:
        return f"{self.project}_obs"

    @property
    def schema_feature(self) -> str:
        return f"{self.project}_feature"

    @property
    def schema_mlops(self) -> str:
        return f"{self.project}_mlops"

    @property
    def schema_llmops(self) -> str:
        return f"{self.project}_llmops"

    def qualified_schema(self, schema_name: str) -> str:
        if self.use_catalog:
            return f"{self.catalog}.{schema_name}"
        return schema_name

    def qualified_table(self, schema_name: str, table_name: str) -> str:
        return f"{self.qualified_schema(schema_name)}.{table_name}"

    @property
    def obs_runs_table(self) -> str:
        return self.qualified_table(self.schema_obs, "log_pipeline_runs")

    @property
    def obs_lineage_table(self) -> str:
        return self.qualified_table(self.schema_obs, "log_pipeline_lineage")

    @property
    def demo_table(self) -> str:
        return self.qualified_table(self.schema_silver, "demo_clientes")
