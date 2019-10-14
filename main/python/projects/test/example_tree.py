from src.main.python.base_nodes import BaseDataSource
import src.main.python.time_utils as tu


class DepA(BaseDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

    def set_dependencies(self):
        self.dependency_list = [
        ]


class NodeA(BaseDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)
        
    def set_dependencies(self):
        self.dependency_list = [
            DepA(self.configuration, period_offset=tu.current())
        ]


class NodeB(BaseDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

    def set_dependencies(self):
        self.dependency_list = [
            NodeA(self.configuration, period_offset=tu.previous(offset=2))
        ]


class NodeC(BaseDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

    def set_dependencies(self):
        self.dependency_list = [
            NodeB(self.configuration, period_offset=tu.previous(offset=1)),
            NodeA(self.configuration, period_offset=tu.previous(offset=2))
        ]


class NodeD(BaseDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

    def set_dependencies(self):
        self.dependency_list = [
            NodeC(self.configuration, period_offset=tu.current())
        ]


class NodeE(BaseDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

    def set_dependencies(self):
        self.dependency_list = [
            NodeD(self.configuration, period_offset=tu.previous(offset=1))
        ]
