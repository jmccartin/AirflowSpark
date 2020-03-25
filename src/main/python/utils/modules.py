import glob
import importlib
import logging
import os
from typing import Dict, List


def node_finder(py_file: str) -> List[str]:
    """
    Finds the name of a class which is declared within a file.
    Much safer than doing a blind import as a module, then getting the name of each object.
    :param py_file: Full path to a python file in which to scan for classes
    :return: name of the class
    """
    class_list = []
    with open(py_file, "r") as f:
        for line in f:
            if line.lstrip().startswith("class"):
                class_name = line.split("(")[0].split(" ")[-1]
                logging.debug(f"Found class: {class_name}")
                class_list.append(class_name)
    return class_list


def import_all_nodes(path: str) -> dict:
    """
    Finds all nodes within the project directory and imports them
    :param path: path to the projects folder
    :return: a dictionary with each item being a list of nodes, keyed by the project name.
    """
    module_dict = {}

    all_node_names = set()

    for dag_dir in glob.glob(os.path.join(path, "*")):
        # Skip init files or pycache directories
        if (not os.path.isdir(dag_dir)) or os.path.basename(dag_dir).startswith("__"):
            continue

        initialised_nodes = {}

        for py_file in glob.glob(os.path.join(dag_dir, "*.py")):
            # Get the python import path of the file itself (ie, src.main.file)
            py_path = os.path.splitext(py_file.replace(os.path.sep, "."))[0]
            if os.path.basename(py_file).startswith("__"):  # skip the __init__.py files
                continue
            for node_name in node_finder(py_file):
                if node_name not in all_node_names:
                    all_node_names.add(node_name)
                else:
                    raise ImportError(f"There is already a defined node class called {node_name}."
                                      f"Please use unique names.")
                module = importlib.import_module(py_path)
                initialised_nodes[node_name] = getattr(module, node_name)

        module_dict[os.path.basename(dag_dir)] = initialised_nodes

    return module_dict


def find_endpoints(d: Dict[str, List[str]]) -> Dict[str, set]:
    """
    Finds the endpoints in a DAG by iterating through a dictionary.
    Endpoints are used to attach the create/destroy cluster nodes.
    :param d: dictionary of the node names, with the names of the dependencies as a value list
    :return: list of the endpoint node names
    """

    endpoints = {
        "start": set(),
        "end": set()
    }

    # Endpoints are the same if the DAG only has one node
    if len(d.keys()) == 1:
        (key, value), = d.items()
        endpoints["start"].add(key)
        endpoints["end"].add(key)
        return endpoints

    rev = {}
    # Loop over all dependencies, only create a 'start' endpoint
    # if there are no dependencies inside the DAG for that node.
    for key, values in d.items():
        has_DAG_dependency = False
        for value in values:
            if value in d.keys():
                has_DAG_dependency = True
            if value not in rev.keys():
                rev[value] = [key]
            else:
                rev[value].append(key)
        if not has_DAG_dependency:
            endpoints["start"].add(key)

    for key in d.keys():
        if key not in rev.keys():
            endpoints["end"].add(key)

    return endpoints


def show_all_project_nodes():

    project_path = os.path.join("src", "main", "python", "projects")

    for project_items in import_all_nodes(path=project_path).items():
        print(f"{project_items[0]}:")
        for node_name, cls in project_items[1].items():
            globals()[node_name] = cls
            print(f"|--{node_name}")
        print("")


if __name__ == "__main__":

    show_all_project_nodes()
