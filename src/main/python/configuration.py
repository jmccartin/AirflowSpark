import yaml


class Configuration:
    def __init__(self, config_file=None, node_name=None):

        with open(config_file, "r") as f:
            conf = yaml.safe_load(f)

        self._conf = conf
        self.config_file = config_file
        self.settings = conf.get("general")
        self.emr = conf.get("emr")
        self.spark = conf.get("spark")
        self.s3 = conf.get("s3")
        self.node_name = node_name

    def get_project_or_default(self, project_name: str):
        """
        Attempts to get any project-level configuration settings for
        Spark and EMR if there are any defined. These are combined
        with the defaults specified in the configuration.
        If extra keys but 'spark' and 'emr' are set for each project,
        an error will be raised.

        :param project_name: Name of the project (must be consistent with directory name)
        :return: this class
        """

        # If there is no specific configuration defined for the
        # project, return the main one
        if project_name not in self._conf.get("projects").keys():
            return self

        project_emr_settings = self._conf.get("projects").get(project_name).get("emr")
        if project_emr_settings is not None:
            for key, value in project_emr_settings.items():
                self.emr[key] = value

        project_spark_settings = self._conf.get("projects").get(project_name).get("spark")
        if project_spark_settings is not None:
            for key, value in project_spark_settings.items():
                self.spark[key] = value

        found_keys = self._conf.get("projects").get(project_name).keys()
        extra_keys = [k for k in found_keys if k not in ["emr", "spark"]]
        if len(extra_keys) > 0:
            raise KeyError("Only 'emr' and 'spark' configuration settings at the project-level are supported.")

        return self
