# Python
import boto3
import glob
import logging
import os
import re
import shutil
from typing import List, Tuple
from urllib.parse import urlparse


def clear_table_partition(table_partition_path: str) -> None:
    """
    Clears the specific partition on the filestore. The filestore
    type (S3, local) is taken from the scheme in the URL.
    :param table_path: URL including the path to the files.
    :return: The number of removed files in in the path.
    """

    s3_resource = None

    url = urlparse(table_partition_path)

    # Check the first path to determine the storage method
    if url.scheme == "s3":
        s3_resource = boto3.resource("s3")

    partition_group = re.findall(r"year=(\d{4})/month=(\d{1,2})", url.path)
    if len(partition_group) != 1:
        raise Exception(f"A malformed path to the table partition has been received <{table_partition_path}>.")
    year, month = partition_group[0]
    logging.warning(f"Found previously generated data for year={year}, month={month}. Removing files.")
    if not s3_resource:
        shutil.rmtree(table_partition_path)
    else:
        bucket = url.netloc
        s3_bucket = s3_resource.Bucket(bucket)
        s3_bucket.objects.filter(Prefix=url.path.lstrip("/")).delete()


def list_table_partitions(table_path: str) -> List[Tuple[str, bool]]:
    """
    Lists all partition paths for that dataset.
    This supports two filestores: local and S3.
    :param table_root_path: Must be a URL path such as:
             - s3://<bucket_name>/path/to/<table_name>/*/
             - file://path/to/<table_name>/*/
    :return: A list of all of the partition paths and whether or
             not they contain a "_SUCCESS" file as a
             two-entry tuple.
    """

    all_paths_set = set()

    url = urlparse(table_path)

    success_id = "_SUCCESS"

    if url.scheme == "s3":
        s3_resource = boto3.resource("s3")
        bucket = url.netloc
        s3_bucket = s3_resource.Bucket(bucket)
        for bucket_object in list(s3_bucket.objects.filter(Prefix=url.path[1:])):
            full_path = url.scheme + f"://{bucket}/" + "/".join(bucket_object.key.split("/")[:-1])
            if success_id in bucket_object.key:
                all_paths_set.add((full_path, True))
            else:
                all_paths_set.add((full_path, False))
    else:
        if not os.path.exists(url.path):
            raise FileNotFoundError(f"The directory {url.path} does not exist.")
        # Find each directory denoted by the month partition, and
        # look inside each directory for the presence of a success file.
        for directory in glob.glob(os.path.join(url.path, "**", "*month=*"), recursive=True):
            path_state = (directory, False)
            for file in os.listdir(directory):
                if file == success_id:
                    path_state = (f"file://{directory}", True)
            all_paths_set.add(path_state)

    return list(all_paths_set)
