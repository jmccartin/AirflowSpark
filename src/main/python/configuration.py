import yaml


class Configuration:
    def __init__(self, config_file=None, node_name=None):

        with open(config_file, "r") as f:
            conf = yaml.safe_load(f)

        self.settings = conf.get("general")
        self.s3 = conf.get("s3")
        self.node_name = node_name
