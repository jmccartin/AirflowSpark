# Airflow
from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.settings import WEB_COLORS
from airflow.utils.decorators import apply_defaults

"""
The code in this file is an override of the airflow methods due to the
bug as reported in AIRFLOW-5744 (https://issues.apache.org/jira/browse/AIRFLOW-5744).
Once the bug has been fixed and merged into a release, this code can safely be deleted.
"""


class SparkSubmitHookEMR(SparkSubmitHook):

    def _build_spark_submit_command(self, application):
        """
        Construct the spark-submit command to execute.
        :param application: command to append to the spark-submit command
        :type application: str
        :return: full command to be executed
        """
        connection_cmd = self._get_spark_binary_path()

        # The url ot the spark master
        connection_cmd += ["--master", self._connection["master"]]

        if self._conf:
            for key in self._conf:
                connection_cmd += ["--conf", "{}={}".format(key, str(self._conf[key]))]
        if self._env_vars and (self._is_kubernetes or self._is_yarn):
            if self._is_yarn:
                tmpl = "spark.yarn.appMasterEnv.{}={}"
            else:
                tmpl = "spark.kubernetes.driverEnv.{}={}"
            for key in self._env_vars:
                connection_cmd += [
                    "--conf",
                    tmpl.format(key, str(self._env_vars[key]))]
            self._env = self._env_vars  # TODO: Get this merged into Airflow project itself
        elif self._env_vars and self._connection["deploy_mode"] != "cluster":
            self._env = self._env_vars  # Do it on Popen of the process
        elif self._env_vars and self._connection["deploy_mode"] == "cluster":
            raise AirflowException(
                "SparkSubmitHook env_vars is not supported in standalone-cluster mode.")
        if self._is_kubernetes:
            connection_cmd += ["--conf", "spark.kubernetes.namespace={}".format(
                self._connection["namespace"])]
        if self._files:
            connection_cmd += ["--files", self._files]
        if self._py_files:
            connection_cmd += ["--py-files", self._py_files]
        if self._archives:
            connection_cmd += ["--archives", self._archives]
        if self._driver_class_path:
            connection_cmd += ["--driver-class-path", self._driver_class_path]
        if self._jars:
            connection_cmd += ["--jars", self._jars]
        if self._packages:
            connection_cmd += ["--packages", self._packages]
        if self._exclude_packages:
            connection_cmd += ["--exclude-packages", self._exclude_packages]
        if self._repositories:
            connection_cmd += ["--repositories", self._repositories]
        if self._num_executors:
            connection_cmd += ["--num-executors", str(self._num_executors)]
        if self._total_executor_cores:
            connection_cmd += ["--total-executor-cores", str(self._total_executor_cores)]
        if self._executor_cores:
            connection_cmd += ["--executor-cores", str(self._executor_cores)]
        if self._executor_memory:
            connection_cmd += ["--executor-memory", self._executor_memory]
        if self._driver_memory:
            connection_cmd += ["--driver-memory", self._driver_memory]
        if self._keytab:
            connection_cmd += ["--keytab", self._keytab]
        if self._principal:
            connection_cmd += ["--principal", self._principal]
        if self._name:
            connection_cmd += ["--name", self._name]
        if self._java_class:
            connection_cmd += ["--class", self._java_class]
        if self._verbose:
            connection_cmd += ["--verbose"]
        if self._connection["queue"]:
            connection_cmd += ["--queue", self._connection["queue"]]
        if self._connection["deploy_mode"]:
            connection_cmd += ["--deploy-mode", self._connection["deploy_mode"]]

        # The actual script to execute
        connection_cmd += [application]

        # Append any application arguments
        if self._application_args:
            connection_cmd += self._application_args

        self.log.info("Spark-Submit cmd: %s", connection_cmd)

        return connection_cmd


class SparkSubmitOperatorEMR(BaseOperator):
    """
    This hook is a wrapper around the spark-submit binary to kick off a spark-submit job.
    It requires that the "spark-submit" binary is in the PATH or the spark-home is set
    in the extra on the connection.

    :param application: The application that submitted as a job, either jar or py file. (templated)
    :type application: str
    :param conf: Arbitrary Spark configuration properties (templated)
    :type conf: dict
    :param conn_id: The connection id as configured in Airflow administration. When an
                    invalid connection_id is supplied, it will default to yarn.
    :type conn_id: str
    :param files: Upload additional files to the executor running the job, separated by a
                  comma. Files will be placed in the working directory of each executor.
                  For example, serialized objects. (templated)
    :type files: str
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py. (templated)
    :type py_files: str
    :param jars: Submit additional jars to upload and place them in executor classpath. (templated)
    :type jars: str
    :param driver_class_path: Additional, driver-specific, classpath settings. (templated)
    :type driver_class_path: str
    :param java_class: the main class of the Java application
    :type java_class: str
    :param packages: Comma-separated list of maven coordinates of jars to include on the
                     driver and executor classpaths. (templated)
    :type packages: str
    :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
                             while resolving the dependencies provided in "packages" (templated)
    :type exclude_packages: str
    :param repositories: Comma-separated list of additional remote repositories to search
                         for the maven coordinates given with "packages"
    :type repositories: str
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
                                 (Default: all the available cores on the worker)
    :type total_executor_cores: int
    :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
    :type executor_cores: int
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :type executor_memory: str
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :type driver_memory: str
    :param keytab: Full path to the file that contains the keytab (templated)
    :type keytab: str
    :param principal: The name of the kerberos principal used for keytab (templated)
    :type principal: str
    :param name: Name of the job (default airflow-spark). (templated)
    :type name: str
    :param num_executors: Number of executors to launch
    :type num_executors: int
    :param application_args: Arguments for the application being submitted (templated)
    :type application_args: list
    :param env_vars: Environment variables for spark-submit. It supports yarn and k8s mode too. (templated)
    :type env_vars: dict
    :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
    :type verbose: bool
    :param spark_binary: The command to use for spark submit.
                         Some distros may use spark2-submit.
    :type spark_binary: string
    """
    template_fields = ("_application", "_conf", "_files", "_py_files", "_jars", "_driver_class_path",
                       "_packages", "_exclude_packages", "_keytab", "_principal", "_name",
                       "_application_args", "_env_vars")
    ui_color = WEB_COLORS["LIGHTORANGE"]

    @apply_defaults
    def __init__(self,
                 application="",
                 conf=None,
                 conn_id="spark_default",
                 files=None,
                 py_files=None,
                 archives=None,
                 driver_class_path=None,
                 jars=None,
                 java_class=None,
                 packages=None,
                 exclude_packages=None,
                 repositories=None,
                 total_executor_cores=None,
                 executor_cores=None,
                 executor_memory=None,
                 driver_memory=None,
                 keytab=None,
                 principal=None,
                 name="airflow-spark",
                 num_executors=None,
                 application_args=None,
                 env_vars=None,
                 verbose=False,
                 spark_binary="spark-submit",
                 *args,
                 **kwargs):
        super(SparkSubmitOperatorEMR, self).__init__(*args, **kwargs)
        self._application = application
        self._conf = conf
        self._files = files
        self._py_files = py_files
        self._archives = archives
        self._driver_class_path = driver_class_path
        self._jars = jars
        self._java_class = java_class
        self._packages = packages
        self._exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = principal
        self._name = name
        self._num_executors = num_executors
        self._application_args = application_args
        self._env_vars = env_vars
        self._verbose = verbose
        self._spark_binary = spark_binary
        self._hook = None
        self._conn_id = conn_id

    def execute(self, context):
        """
        Call the SparkSubmitHook to run the provided spark job
        """
        self._hook = SparkSubmitHookEMR(
            conf=self._conf,
            conn_id=self._conn_id,
            files=self._files,
            py_files=self._py_files,
            archives=self._archives,
            driver_class_path=self._driver_class_path,
            jars=self._jars,
            java_class=self._java_class,
            packages=self._packages,
            exclude_packages=self._exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            name=self._name,
            num_executors=self._num_executors,
            application_args=self._application_args,
            env_vars=self._env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary
        )
        self._hook.submit(self._application)

    def on_kill(self):
        self._hook.on_kill()
