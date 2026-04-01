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
    def log_table(self) -> str:
        if self.use_catalog:
            return f"{self.catalog}.{self.schema_obs}.log_platform"
        return f"{self.schema_obs}.log_platform"

    @property
    def demo_table(self) -> str:
        if self.use_catalog:
            return f"{self.catalog}.{self.schema_silver}.demo_clientes"
        return f"{self.schema_silver}.demo_clientes"