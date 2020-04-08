# Airflow
from airflow import AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.models import DAG

# Python
import os
from typing import List, Tuple

# Module
from src.main.python.configuration import Configuration
from src.main.python.utils.emr import create_yarn_dir

configuration = Configuration(config_file=os.path.join("config", "configuration.yml"))
yarn_tmp_dir = os.path.join(os.path.sep, "tmp", "airflow")

# -------------------
# -- Configuration --
# -------------------

if configuration.settings.get("aws"):
    # Use the EMR provided Spark binaries, not from Python lib
    spark_binary = "/usr/bin/spark-submit"
else:
    spark_binary = "spark-submit"


class EmrCreatedSensor(EmrJobFlowSensor):
    """
    Asks for the state of the JobFlow until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    This class overrides the poke method from the EmrBaseSensor
    in order to determine the IP address and hostname of the cluster
    master ec2 instance, which is then written to the project-level
    `yarn-site.xml` file.

    The states 'WAITING' and 'RUNNING' are removed from the class
    var NON_TERMINAL_STATE, as once the cluster has been bootstrapped,
    the sensor should finish running.

    :param job_flow_id: job_flow_id of which to check the state
    :param project_name: name of the project DAG
    """

    NON_TERMINAL_STATES = ["STARTING", "BOOTSTRAPPING", "TERMINATING"]
    PROVISIONED_STATES = ["WAITING", "READY", "RUNNING"]

    def __init__(self,
                 job_flow_id,
                 project_name,
                 *args,
                 **kwargs):
        super().__init__(job_flow_id=job_flow_id, *args, **kwargs)
        self.project_name = project_name

    @staticmethod
    def master_address_from_response(response) -> str:
        return response["Cluster"]["MasterPublicDnsName"]

    def get_yarn_project_dir(self):
        tmp_dir = os.path.join(yarn_tmp_dir, self.project_name)
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        return tmp_dir

    def poke(self, context):
        response = self.get_emr_response()

        if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            self.log.info(f"Bad HTTP response: {response}")
            return False

        state = self.state_from_response(response)
        self.log.info(f"Job flow currently {state}")

        if state in self.NON_TERMINAL_STATES:
            return False

        if state in self.PROVISIONED_STATES:
            # If the cluster has been provisioned, the IP address of the
            # master should be known, and so it can be written to the
            # project-level `yarn-site.xml` file.
            master_hostname = self.master_address_from_response(response)
            create_yarn_dir(
                yarn_dir=self.get_yarn_project_dir(),
                hostname=master_hostname
            )
            self.log.info(f"Created yarn configuration files with master: {master_hostname}")
            return True

        if state in self.FAILED_STATE:
            final_message = "EMR job failed"
            failure_message = self.failure_message_from_response(response)
            if failure_message:
                final_message += " " + failure_message
            raise AirflowException(final_message)

        return True


def create_emr_cluster_operator(dag: DAG):

    # EMR Cluster
    project_settings = configuration.get_project_or_default(project_name=dag.dag_id)

    s3_bucket = project_settings.s3.get("bucket")
    security_groups = project_settings.emr.get("additionalMasterSecurityGroups")

    instances = {
        "InstanceGroups": [
            {
                "InstanceRole": "MASTER",
                "InstanceType": project_settings.emr.get("masterInstanceType", "m4.large"),
                "InstanceCount": 1
            },
            {
                "InstanceRole": "CORE",
                "InstanceType": project_settings.emr.get("coreInstanceType"),
                "InstanceCount": project_settings.emr.get("coreInstanceNum"),
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": project_settings.emr.get("ebsRootVolumeSize", 50),
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }
                    ],
                    "EbsOptimized": True
                }
            },

        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "Ec2SubnetId": project_settings.emr.get("ec2SubnetId"),
        "AdditionalMasterSecurityGroups": security_groups,
        "AdditionalSlaveSecurityGroups": project_settings.emr.get("additionalSlaveSecurityGroups", security_groups),
        "Ec2KeyName": project_settings.emr.get("ec2KeyName"),
    }

    bootstrap_script = project_settings.emr.get("bootstrapScriptName")

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

    cluster_name = "Airflow Project=" + dag.dag_id.replace("_", "-")

    emr_job_flow_dict = {
        "Name": cluster_name,
        "ReleaseLabel": project_settings.emr.get("releaseLabel"),
        "Instances": instances,
        "BootstrapActions": bootstrapActions,
        "Steps": steps,
        "VisibleToAllUsers": True
    }

    return EmrCreateJobFlowOperator(
        task_id="create_emr_cluster_operator",
        aws_conn_id="aws_default",  # Configured via Admin Connections page
        emr_conn_id="emr_default",
        job_flow_overrides=emr_job_flow_dict,
        region_name=project_settings.emr.get("regionName"),
        retries=0,
        dag=dag,
    )


def emr_cluster_sensor(dag: DAG, project_name: str):

    return EmrCreatedSensor(
        task_id="emr_cluster_sensor",
        project_name=project_name,
        aws_conn_id="aws_default",  # Configured via Admin Connections page
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster_operator', key='return_value') }}",
        dag=dag
    )


def terminate_emr_cluster_operator(dag: DAG):

    return EmrTerminateJobFlowOperator(
        task_id="destroy_emr_cluster_operator",
        aws_conn_id="aws_default",  # Configured via Admin Connections page
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster_operator', key='return_value') }}",
        trigger_rule="none_skipped",
        retries=0,
        dag=dag
    )


def submit_spark_job(dag: DAG,
                     node_name: str,
                     offsets_pair: Tuple[int, int],
                     execution_date: str):
    """
    Submits a spark job to the cluster.
    :param dag: The Airflow DAG of the associated project
    :param node_name: The name of the node in the DAG. Typically the recipe class name.
    :param offsets_pair: A tuple pair of the total and period offset. These are integers
    representing the total period (typically months) between the DAG execution date and
    the date to run for the node itself. The period offset is relative to the node one
    position forward in the project DAG.
    :param execution_date: date for which the job is being run (YYYY-mm-dd) (templated)
    :return: an Airflow Spark Operator
    """

    # For formatting of the task_id in the airflow DAG
    def offset2str(offset):
        if offset == 0:
            return "0"
        else:
            return f"-{offset}"

    spark_application_args = [
        node_name,
        execution_date,
        str(offsets_pair[0]),
        str(offsets_pair[1])
    ]

    # Project-level spark configuration
    project_settings = configuration.get_project_or_default(project_name=dag.dag_id)

    spark_conf = {
        "queue": project_settings.spark.get("queue", "default"),
        "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties",
        "spark.executor.cores": project_settings.spark.get("executor_cores", 2),
        "spark.executor.memory": project_settings.spark.get("executor_memory", "4g"),
        "spark.executor.memoryOverhead": project_settings.spark.get("executor_memoryOverhead", 1200),
        "spark.pyspark.python": project_settings.spark.get("pyspark_python"),
        "spark.pyspark.driver.python": project_settings.spark.get("pyspark_driver_python"),
        "spark.sql.shuffle.partitions": "50",
        "spark.sql.execution.arrow.enabled": "true",
        "spark.eventLog.dir": "hdfs:///var/log/spark/apps"
    }

    # Validate the configuration
    for key, var in spark_conf.items():
        if not var:
            raise Exception(f"Missing configuration value for key {key} in "
                            f"config file: {configuration.config_file}.")

    # The project name should always be the id of the parent-most DAG
    project_name = dag.dag_id.split(".")[0]
    yarn_project_dir = os.path.join(yarn_tmp_dir, project_name)

    env_vars = {
        "SPARK_CONF_DIR": yarn_project_dir,
        "HADOOP_CONF_DIR": yarn_project_dir,
        "YARN_CONF_DIR": yarn_project_dir
    }

    # Pretty name for the Spark UI page. Since 'execution_date'
    # is jinja templated, we can't manipulate the string here.
    spark_ui_name = f"{node_name} Date={execution_date}"

    # Python and other files sent to the driver and executors
    project_files = os.path.join("target", "project_files.zip")
    py_files = f"{project_files},config/spark/log4j.properties"

    return SparkSubmitOperator(
        application=os.path.join("src", "main", "python", "spark_main.py"),
        name=spark_ui_name,
        conf=spark_conf,
        conn_id="spark_default",  # Points to the 'connections' page in Admin (airflow). Seems to be ignored.
        py_files=py_files,
        application_args=spark_application_args,
        task_id=f"{node_name}.Month.{offset2str(offsets_pair[0])}",
        verbose=False,
        dag=dag,
        env_vars=env_vars,
        spark_binary=spark_binary,
        retries=0,
        pool="spark_sequential"
    )
