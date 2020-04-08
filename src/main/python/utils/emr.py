# Python
import boto3
import logging
import os
import time
from typing import TextIO

# Module
from src.main.python.configuration import Configuration

configuration = Configuration(config_file=os.path.join("config", "configuration.yml"))
region_name = configuration.emr.get("regionName")
security_groups = configuration.emr.get("additionalMasterSecurityGroups")
yarn_tmp_dir = os.path.join(os.path.sep, "tmp", "airflow")


def map_config_file(f1: TextIO, f2: TextIO, hostname: str) -> None:
    """
    Extracts an IPv4 address from a valid hostname string, and
    maps it on to a file f1 to a new file f2.
    """

    ip = hostname.split(".")[0].lstrip("ip-").replace("-", ".")
    for line in f1.readlines():
        if "AWS-EMR-MASTER-FQDN" in line:
            mapped_line = line.replace("AWS-EMR-MASTER-FQDN", hostname)
        elif "AWS-EMR-MASTER-IP" in line:
            mapped_line = line.replace("AWS-EMR-MASTER-IP", ip)
        else:
            mapped_line = line
        f2.write(mapped_line)


def create_yarn_dir(yarn_dir: str, hostname: str) -> None:
    """
    Creates the YARN and Spark conf directory with all of the relevant
    configurations mapped to the hostname of the cluster master.
    :param yarn_dir: String of the yarn directory. Must be created.
    :param hostname: Cluster master address
    """
    # Copy and/or map the yarn files according to the project requirements
    config_files = ["core-site.xml", "yarn-site.xml", "spark-env.sh", "spark-defaults.conf"]
    for file in config_files:
        with open(os.path.join("config", "spark", file), "r") as f1, open(os.path.join(yarn_dir, file), "w") as f2:
            map_config_file(f1, f2, hostname)


def create_emr_cluster(cluster_name: str = "Airflow Test") -> str:
    """
    Creates an EMR cluster with a default set of configuration values.
    :param cluster_name: The name to give to the cluster.
    :return: the JobFlowId of the cluster
    """

    emr_client = boto3.client("emr", region_name=region_name)
    security_groups = configuration.emr.get("additionalMasterSecurityGroups")

    clusters_response = emr_client.list_clusters(
        ClusterStates=[
            "STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"
        ]
    )

    for cluster in clusters_response["Clusters"]:
        id = cluster["Id"]
        if cluster["Name"] == cluster_name:
            raise Exception(f"There already exists a cluster for testing purposes. (Id: {id})")

    instances = {
        "InstanceGroups": [
            {
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1
            },
            {
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1
            },

        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "Ec2SubnetId": configuration.emr.get("ec2SubnetId"),
        "AdditionalMasterSecurityGroups": security_groups,
        "AdditionalSlaveSecurityGroups": configuration.emr.get("additionalSlaveSecurityGroups", security_groups),
        "Ec2KeyName": configuration.emr.get("ec2KeyName"),
    }

    bootstrap_script = configuration.emr.get("bootstrapScriptName")
    s3_bucket = configuration.s3.get("bucket")

    bootstrapActions = [
        {
            "Name": "Custom action",
            "ScriptBootstrapAction": {
                "Path": f"s3://{s3_bucket}/bootstrap/{bootstrap_script}",
            }
        }
    ]

    steps = [
        {
            "Name": "SetupAirflowUser",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "s3://elasticmapreduce/libs/script-runner/script-runner.jar",
                "Args": [
                    f"s3://{s3_bucket}/bootstrap/setup_airflow_user.sh",
                ]
            }
        },
    ]

    applications = [
        {
            "Name": "Spark"
        },
        {
            "Name": "Hadoop"
        }
    ]

    # Set the default tags and fill the rest from the configuration
    tags = [
        {
            "Key": "Name",
            "Value": cluster_name
        }
    ]
    for tag_dict in configuration.emr.get("tags"):
        for key, value in tag_dict.items():
            tags.append({"Key": key, "Value": value})

    response = emr_client.run_job_flow(
        Name=cluster_name,
        LogUri=f"s3://{s3_bucket}/logs",
        ReleaseLabel=configuration.emr.get("releaseLabel"),
        Instances=instances,
        Steps=steps,
        BootstrapActions=bootstrapActions,
        Applications=applications,
        ServiceRole=configuration.emr.get("serviceRole"),
        JobFlowRole=configuration.emr.get("jobFlowRole"),
        Tags=tags,
        VisibleToAllUsers=True
    )

    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise Exception(f"There was an unsuccessful response returned: {response}", )

    return response["JobFlowId"]


def await_cluster_hostname(id: str, wait_period=60) -> str:

    emr_client = boto3.client("emr", region_name=region_name)
    ready_states = ["RUNNING", "WAITING"]

    logging.info(f"Waiting every {wait_period} seconds...")

    def get_state():
        response = emr_client.describe_cluster(ClusterId=id)
        state = response["Cluster"]["Status"]["State"]
        now = time.strftime("%H:%M:%S")
        logging.info(f"[{now}] Cluster state is: {state}")
        return state

    state = get_state()

    # Wait on the cluster until it is in a ready state.
    while state not in ready_states:
        time.sleep(wait_period)
        state = get_state()

    logging.info("Cluster has been provisioned. Exiting sensor...")

    return emr_client.describe_cluster(ClusterId=id)["Cluster"]["MasterPublicDnsName"]
