# Python libraries
from abc import ABC, abstractmethod
import boto3
from datetime import date
from dateutil.relativedelta import relativedelta
import inspect
import glob
import logging
import os
import shutil
from typing import Any, List, Tuple

# PySpark
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.types import StructType, StructField, IntegerType

# Project files
from src.main.python.spark_main import setup_sparkSession
from src.main.python.time_utils import current, chronological_sorter, Static


class AbstractNode(ABC):
    @abstractmethod
    def generate_node(self, year, month) -> DataFrame:
        pass

    @abstractmethod
    def set_dependencies(self):
        pass


class BaseTableNode(AbstractNode):
    """
    Base class for all interactions with a parquet-based table
    """

    def __init__(self, configuration, period_offset):
        self._hdfs_paths = None
        self._schema = None
        self.configuration = configuration
        self._sparkSession = self._get_sparkSession()
        self.project_name = inspect.getfile(self.__class__).split("/")[-2]
        self.base_path = self.get_base_path()
        self.table_path = os.path.join(self.base_path, self.project_name, self.__class__.__name__)
        self.period_offset = period_offset
        self.parent_offset = 0
        self.dependency_list = []
        self.set_dependencies()

    def __str__(self):
        return f"{self.__class__.__name__}:offset={self.total_offset}"

    __repr__ = __str__

    def _get_sparkSession(self):
        return setup_sparkSession(self.configuration.node_name, self.configuration.settings.get("deploy_mode"))

    def get_base_path(self) -> str:
        """
        Gets the prefix for the file store location
        """
        base_path = self.configuration.settings.get("base_path")
        # Append the bucket and s3 prefix to the path
        if self.configuration.settings.get("aws"):
            bucket = self.configuration.s3.get("bucket")
            return f"s3://{bucket}/{base_path}/{self.project_name}"
        else:
            return os.path.join("file:", base_path, self.project_name)

    @property
    def schema(self) -> StructType:
        return self._schema

    @schema.setter
    def schema(self, struct: StructType) -> None:
        if type(struct) != StructType:
            raise TypeError("The schema is not valid!")
        struct = struct.add(StructField("year", IntegerType(), False))
        struct = struct.add(StructField("month", IntegerType(), False))
        self._schema = struct

    def fit_to_schema(self, dataframe: DataFrame) -> DataFrame:
        """
        Ensures that the dataframe returned at the end of generate_node
        matches the schema defined in the class. If extra columns are
        present, these are dropped from the ouput.
        """
        select_cols = []
        for field in self.schema:
            if field.name not in ["year", "month"]:
                if field.name not in dataframe.columns:
                    raise IndexError(f"Can not find schema column: {field.name} in output dataframe.")
                col_schema = dataframe.select(field.name).schema[0]
                if field.dataType != col_schema.dataType:
                    raise TypeError(f"Column: {field.name} has an unexpected type "
                                    f"(expected {field.dataType}, found {col_schema.dataType}).")
                select_cols.append(col_schema.name)
        return dataframe.select(select_cols)

    def verify_schema(self, dataframe: DataFrame) -> None:
        """
        Similar to fit_to_schema, only this method verifies the schema, and
        if it doesn't match the schema defined in the class, an exception
        is thrown.
        :param dataframe: the spark dataframe on which to verify the schema
        :return:
        """
        schema_fields = [field.name for field in self.schema if field.name not in ["year", "month"]]
        if sorted(schema_fields) != sorted(dataframe.columns):
            raise IndexError(f"The data for the node: <{self.__class__.__name__}> with columns: {dataframe.columns} "
                             f"does not match the defined schema {schema_fields}")
        for field in self.schema:
            if field.name not in ["year", "month"]:
                col_schema = dataframe.select(field.name).schema[0]
                if field.dataType != col_schema.dataType:
                    raise TypeError(f"Column: {field.name} has an unexpected type "
                                    f"(expected {field.dataType}, found {col_schema.dataType}).")

    def get_dataframe(self, year, month) -> DataFrame:
        """
        Returns a dataframe for the given dependency over the given period.
        :return: Spark SQL DataFrame
        """
        self.set_paths(year, month)
        dependency_df = self._sparkSession.read.parquet(str(*self._hdfs_paths))
        self.verify_schema(dependency_df)

        return dependency_df

    def set_parent_offset(self, parent_offset):
        self.parent_offset = parent_offset

    @property
    def total_offset(self):
        return self.period_offset + self.parent_offset

    def set_paths(self, year, month):
        # Get the dependency dates that are offset relative to the node being run
        dep_date = date(year, month, 1) - relativedelta(months=self.period_offset)
        self._hdfs_paths = [
            os.path.join(self.base_path,
                         self.__class__.__name__,
                         f"year={dep_date.year}",
                         f"month={dep_date.month}")
        ]


    def evaluate_paths(self, year, month):
        """
        Determines the paths on HDFS/local filesystem with respect to the run date
        :param year: year (int) of the run date
        :param month: month (int) of the run date
        """

        if self.configuration.settings.get("aws"):
            bucket = self.configuration.s3.get("bucket")
            logging.info(f"Running on AWS. Getting paths for S3 bucket: {bucket}.")
            s3 = boto3.resource("s3")
            s3_bucket = s3.Bucket(bucket)
            all_paths_set = set()
            for bucket_object in s3_bucket.objects.filter(Prefix=self.configuration.settings.get("base_path")):
                path = "/".join(bucket_object.key.split("/")[:-1])
                if self.configuration.settings.get("force_regen"):
                    # Only consider successfully completed paths
                    if "_SUCCESS" in bucket_object.key:
                        all_paths_set.add(path)
                    else:
                        all_paths_set.add(path)
            all_paths = list(all_paths_set)
        else:
            logging.info("Running on local environment. S3 disabled.")
            if not os.path.exists(self.base_path):
                raise FileNotFoundError(f"The directory {self.base_path} does not exist")
            all_paths_set = set()
            for path in glob.glob(os.path.join(self.table_path, "*", "*", "*")):
                if self.configuration.settings.get("force_regen"):
                    # Only consider successfully completed paths
                    if "_SUCCESS" in path:
                        all_paths_set.add(path)
                else:
                    all_paths_set.add(path)
            all_paths = list(all_paths_set)

        filtered_paths = list(filter(lambda p: chronological_sorter(p) <= f"{year}.{month:02d}", all_paths))
        chronological_paths = sorted(filtered_paths, key=chronological_sorter)
        neat_order = "\n" + "\n".join(chronological_paths)
        logging.debug(f"Returning paths: {neat_order}")
        self._hdfs_paths = chronological_paths[-(self.total_offset() + 1):]

    def get_period(self) -> Tuple[str, str]:
        pass

    def write_table(self, year, month) -> None:
        """
        Writes the dataset to the chosen table store using partitions of year and
        month, as a series of parquet files. Will overwrite any existing partition
        if the node period is static.
        """
        output_df = (
            self.generate_node(year, month)
            .withColumn("year", f.lit(year))
            .withColumn("month", f.lit(month))
        )

        # Remove any previously-generated files for the month for that node
        node_current = os.path.join(self.table_path, f"year={year}", f"month={month}")
        if self.configuration.settings.get("aws"):
            s3 = boto3.resource("s3")
            bucket = self.configuration.s3.get("bucket")
            s3_bucket = s3.Bucket(bucket)
            keys = s3_bucket.objects.filter(Prefix=node_current)
            if len(keys) != 0:
                logging.warning(f"Found previously generated data for year={year}, month={month}. Removing files.")
                # TODO: Fix this!
                s3_bucket.delete_key(*keys)
        else:
            if os.path.exists(node_current):
                logging.warning(f"Found previously generated data for year={year}, month={month}. Removing files.")
                shutil.rmtree(node_current)

        output_df.write.parquet(path=self.table_path, partitionBy=["year", "month"], mode="append")


class BaseDataSource(BaseTableNode):
    """
    Class for which there is no dependency, and therefore no generate method required.
    """

    def generate_node(self, year, month) -> DataFrame:
        pass

    def fit_to_schema(self, dataframe) -> DataFrame:
        pass

    def write_table(self, year, month) -> None:
        pass
