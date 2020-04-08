# PySpark
from pyspark.sql import SparkSession

# Python
import boto3
from botocore.exceptions import ClientError
from moto import mock_emr, mock_s3
import os
import pytest
import tempfile

# Module
from src.main.python.configuration import Configuration


def setup_sparkSession(n_threads: int = 2) -> SparkSession:

    # Initialise the Spark Context
    return (
        SparkSession
        .builder
        .appName("PyTest")
        .master(f"local[{n_threads}]")
        .getOrCreate()
    )


spark = setup_sparkSession()
configuration = Configuration(config_file=os.path.join("config", "test_configuration.yml"), node_name="PyTest")
bucket = configuration.s3.get("bucket")

sc = spark.sparkContext
tmp_dir = tempfile.TemporaryDirectory(prefix="spark_pytest_")
sc.setCheckpointDir(tmp_dir.name)


@pytest.fixture(scope="session")
def aws_credentials():
    """
    Mocked AWS Credentials for moto.
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "test_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test_secret_access_key"
    os.environ["AWS_SECURITY_TOKEN"] = "test_security_token"
    os.environ["AWS_SESSION_TOKEN"] = "test_session_token"


@pytest.fixture(scope="session")
def s3_client(aws_credentials):
    with mock_s3():
        s3_client = boto3.client(
            "s3",
            region_name="eu-west-1",
        )

        # Ensure that the stubbed calls are not getting out to AWS
        # by checking the existence of the test bucket.
        try:
            s3_resource = boto3.resource(
                "s3",
                region_name="eu-west-1",
            )
            s3_resource.meta.client.head_bucket(Bucket=bucket)
        except ClientError:
            pass
        else:
            raise EnvironmentError(f"The bucket <{bucket}> should not exist in this test environment.")

        s3_client.create_bucket(Bucket=bucket)

        yield s3_client


@pytest.fixture(scope="session")
def emr_client(aws_credentials):
    with mock_emr():
        yield boto3.client(
            "emr",
            region_name="eu-west-1",
        )
