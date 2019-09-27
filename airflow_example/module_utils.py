import glob
import importlib
import logging
import os
from typing import List


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
                logging.info(f"found class: {class_name}")
                class_list.append(class_name)
    return class_list


def import_all_nodes(path):
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

        initialised_nodes = []

        for py_file in glob.glob(os.path.join(dag_dir, "*.py")):
            py_path = py_file.replace(os.path.sep, ".").rstrip(".py")
            if os.path.basename(py_file).startswith("__"):  # skip the __init__.py files
                continue
            for node_name in node_finder(py_file):
                if node_name not in all_node_names:
                    all_node_names.add(node_name)
                else:
                    raise ImportError(f"There is already a defined node class called {node_name}."
                                      f"Please use unique names.")
                module = importlib.import_module(py_path)
                initialised_nodes.append(getattr(module, node_name))

        module_dict[os.path.basename(dag_dir)] = initialised_nodes

    return module_dict
