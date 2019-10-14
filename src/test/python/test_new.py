class BaseNode:

    def __init__(self, offset):
        self.offset = offset
        self.parent_offset = 0

        self.dependency_list = []
        self.set_dependencies()

    @property
    def name(self):
        return self.__class__.__name__

    # def set_offset(self, offset):
    #     self.offset = offset

    def set_parent_offset(self, parent_offset):
        self.parent_offset = parent_offset

    @property
    def total_offset(self):
        return self.offset + self.parent_offset

    def __str__(self):
        return f"{self.name}:offset={self.total_offset}"

    __repr__ = __str__


class NodeE(BaseNode):

    def set_dependencies(self):
        pass


class NodeD(BaseNode):

    def set_dependencies(self):
        self.dependency_list = [
            NodeE(offset=1)
        ]


class NodeC(BaseNode):

    def set_dependencies(self):
        self.dependency_list = [
            NodeD(offset=2)
        ]


class NodeB(BaseNode):

    def set_dependencies(self):
        self.dependency_list = [
            NodeC(offset=1),
            NodeD(offset=0)
        ]


class NodeA(BaseNode):

    def set_dependencies(self):
        self.dependency_list = [
            NodeB(offset=2)
        ]

def get_all_paths(node, path=None):
    paths = []
    if path is None:
        path = []
    path.append(node)
    if node.dependency_list:
        for dep in node.dependency_list:
            dep.set_parent_offset(parent_offset=node.total_offset)
            paths.extend(get_all_paths(dep, path[:]))
    else:
        paths.append(path)
    return paths


root_node = NodeA(0)
paths = get_all_paths(node=root_node)
for path in paths:
    print(path)