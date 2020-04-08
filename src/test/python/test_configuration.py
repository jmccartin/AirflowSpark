from src.main.python.configuration import Configuration

conf = Configuration(config_file="config/test_configuration.yml", node_name="PyTest")


def test_Configuration():
    """
    Tests the airflow configuration for project-specific overrides
    """

    emr = conf.get_project_or_default(project_name="test_project").emr

    assert emr.get("masterInstanceType") == "m4.4xlarge", "The project-level emr setting was not returned"
    assert emr.get("releaseLabel") == "emr-5.24.0", "The default emr setting for releaseLabel was not returned"
