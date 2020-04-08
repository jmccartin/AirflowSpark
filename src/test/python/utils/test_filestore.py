# Module
from src.main.python.utils.filestore import list_table_partitions, clear_table_partition
from src.test.python.test_setup import *


def create_file_structure(table_name):
    """
    Creates a list of files to be created for a unit test
    under the table_name argument.
    """

    table_path = f"{table_name}/parquet/year=2020"

    return [
        f"{table_path}/month=8/data.parquet",
        f"{table_path}/month=8/_SUCCESS",
        f"{table_path}/month=9/data.parquet",
        f"{table_path}/month=9/_SUCCESS",
        f"{table_path}/month=10/data.parquet",  # Deliberately no SUCCESS file for this path
    ]


def test_bucket_number(s3_client):
    result = s3_client.list_buckets()
    assert len(result["Buckets"]) == 1, "There should only be a single created bucket"


def test_local_file_interactions(tmpdir):
    """
    This is designed to test interactions with the a local
    filesystem, such as the listing and removing of files as
    a part of normal dataset operations.
    """

    table_name = "TestProjectTableName"
    table_files = create_file_structure(table_name)

    # Create the paths and write empty data into each of the files
    for file in table_files:
        path = tmpdir.join(*file.split("/"))
        path.write(None, ensure=True)

    table_path = f"file://{tmpdir.join(table_name)}"

    found_paths = list_table_partitions(table_path)
    completed_job_partitions = [p[0] for p in found_paths if p[1]]
    failed_job_partitions = [p[0] for p in found_paths if not p[1]]
    assert len(completed_job_partitions) == 2, "There should only be two partitions with SUCCESS files present."

    for path in failed_job_partitions:
        clear_table_partition(table_partition_path=path)
        assert not os.path.exists(path), "The partition was not successfully removed."


def test_s3_file_interactions(tmpdir, s3_client):
    """
    This is designed to test interactions with the configured
    s3 bucket, such as the listing and removing of keys as
    a part of normal dataset operations.
    """

    bucket = configuration.s3.get("bucket")

    table_name = "TestProjectTableName"
    table_files = create_file_structure(table_name)

    # Create the paths and write empty data into each of the files
    for file in table_files:
        path = tmpdir.join(*file.split("/"))
        path.write(None, ensure=True)
        key = os.path.relpath(path, str(tmpdir))
        s3_client.upload_file(Filename=str(path), Bucket=bucket, Key=key)

    s3_path = f"s3://{bucket}/{table_name}/parquet"

    found_paths = list_table_partitions(table_path=s3_path)
    all_job_partitions = [p[0] for p in found_paths]
    completed_job_partitions = [p[0] for p in found_paths if p[1]]
    assert len(completed_job_partitions) == 2, "There should only be two partitions with SUCCESS files present."

    for path in all_job_partitions:
        clear_table_partition(table_partition_path=path)

    bucket_metadata = s3_client.list_objects(Bucket=bucket)
    assert "Contents" not in bucket_metadata.keys(), "There were still files left in the bucket"
