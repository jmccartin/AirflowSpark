# Python
import pytest

# Module
from src.test.python.test_setup import *
from src.main.python.utils.emr import await_cluster_hostname, create_emr_cluster, create_yarn_dir


def test_cluster_provisioning(tmpdir, emr_client):
    """
    Tests the creation of an EMR cluster via function calls,
    as well as the mapping of relevant config files to the
    hostname of the created cluster.
    """

    cluster_id = create_emr_cluster()
    hostname = await_cluster_hostname(id=cluster_id)
    yarn_dir = tmpdir.mkdir("config")
    # All of the expected files to be mapped
    create_yarn_dir(yarn_dir=yarn_dir, hostname=hostname)

    # Check that every config file for the project was copied across to the yarn dir.
    assert len(yarn_dir.listdir()) == 4, "Not all of the expected files to be mapped were found."

    # Try and create another identical cluster, and ensure that an error is received.
    with pytest.raises(Exception, match="There already exists a cluster for testing purposes."):
        create_emr_cluster()

    # Check one of the lines to be mapped in one of the files that needs hostname mapping.
    mapped_file = yarn_dir.join("yarn-site.xml")
    for line in mapped_file.readlines():
        if ":20888</value>" in line:
            # Moto should return a mocked hostname of the type 'ec2-<IP>-<region>-compute.amazonaws.com'
            # The IP will be random, so just the suffix of the hostname should be checked.
            assert "compute.amazonaws.com" in line, \
                "The placeholder was not correctly mapped to the cluster hostname in the yarn config file."
