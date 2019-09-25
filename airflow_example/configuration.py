import yaml


class Configuration:
    def __init__(self, config_file):
        with open(config_file, "r") as f:
            conf = yaml.safe_load(f)

        self.settings = conf.get("general")


if __name__ == "__main__":
    conf = Configuration("config/configuration.yml")
    import inspect

    print(inspect.getfile(conf.__class__).split("/")[-2])
    # print(conf.settings.get("base_path"))
