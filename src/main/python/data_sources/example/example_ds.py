from src.main.python.base_nodes import ExternalDataSource


class ExampleDataSource1(ExternalDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)
        self.set_external_dependency(table_path="/path/to/data")
