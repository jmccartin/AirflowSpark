# PySpark
from pyspark.sql import DataFrame, functions as f, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.utils import AnalysisException

# Python libraries
from abc import ABC, abstractmethod
import boto3
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import inspect
import glob
import logging
import os
import yaml

# Project files
from src.main.python.utils.filestore import list_table_partitions, clear_table_partition
from src.main.python.utils.time import chronological_sorter


class AbstractNode(ABC):

    @abstractmethod
    def generate_node(self, year: int, month: int) -> DataFrame:
        raise NotImplementedError


class BaseTableNode(AbstractNode):
    """
    Base class for all interactions with a parquet-based table.

    The Spark Session can't be initialised in the init due to Airflow needing access
    to various class properties (fills the server with JVM processes). Therefore it is
    set to None here and is initialised just before it's needed in the various methods.
    """

    def __init__(self, configuration, period_offset):
        self._hdfs_paths = None
        self._schema = None
        self._sparkSession = None
        self._dependency_list = []
        self.configuration = configuration
        self.project_name: str = inspect.getfile(self.__class__).split("/")[-2]
        self.base_path: str = self.get_base_path()
        self.table_path: str = os.path.join(self.base_path, self.__class__.__name__)
        self.period_offset = period_offset
        self.parent_offset = 0

    def __str__(self):
        return f"{self.__class__.__name__}:offset={self.total_offset}"

    __repr__ = __str__

    def init_sparkSession(self):
        self._sparkSession = SparkSession.builder.getOrCreate()

    def get_base_path(self) -> str:
        """
        Gets the prefix for the file store location
        """

        base_path = self.configuration.settings.get("base_path")
        # Append the bucket and s3 prefix to the path
        if self.configuration.settings.get("aws"):
            bucket = self.configuration.s3.get("bucket")
            return f"s3://{bucket}/{base_path}/projects/{self.project_name}"
        else:
            return os.path.join("file:", base_path, self.project_name)

    def get_project_configuration(self) -> dict:
        """
        Gets the project-level configuration.
        Configuration files must be placed in a similar folder structure as
        to the project, under the config directory, with name 'configuration.yml'.

        If running tests, the configurations will be read from a similar
        directory tree, only starting from the 'test' folder. This is useful
        for testing methods such as loops through a list, with only a single
        entry instead of a large list in normal operation.
        :return: Loaded configuration (dict-like object).
        """

        # TODO: Remove explicit checking for test cases!
        if self.configuration.settings.get("is_test"):
            path = os.path.join("config", "test", "projects", self.project_name, "configuration.yml")
        else:
            path = os.path.join("config", "projects", self.project_name, "configuration.yml")
        with open(path, "r") as config_file:
            return yaml.safe_load(config_file)

    @property
    def dependency_list(self):
        return self._dependency_list

    @dependency_list.setter
    def dependency_list(self, d_list):
        self._dependency_list = d_list

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
        if not self.schema:
            raise Exception(f"You have not specified a schema for the class: {self.__class__.__name__}")
        for field in self.schema:
            if field.name not in ["year", "month"]:
                if field.name not in dataframe.columns:
                    dataframe = dataframe.withColumn(field.name, f.lit(None).cast(field.dataType))
                    logging.warning(f"The column '{field.name}' was not found in output dataframe. "
                                    f"Appending null values.")
                col_schema = dataframe.select(field.name).schema[0]
                if field.dataType != col_schema.dataType:
                    raise TypeError(f"The column '{field.name}' has an unexpected type "
                                    f"(expected {field.dataType}, found {col_schema.dataType}).")
                select_cols.append(col_schema.name)
        return dataframe.select(select_cols)

    def get_dataframe(self, year, month) -> DataFrame:
        """
        Returns a dataframe for the given dependency over the given period.
        :return: Spark SQL DataFrame
        """
        if not self._sparkSession:
            self.init_sparkSession()
        self.set_paths(year, month)
        dependency_df = self._sparkSession.read.parquet(str(*self._hdfs_paths))

        return dependency_df

    def set_parent_offset(self, parent_offset):
        self.parent_offset = parent_offset

    @property
    def total_offset(self):
        return self.period_offset + self.parent_offset

    def set_paths(self, year, month):
        """
        Sets the hdfs paths of the dependency to that of the dates
        which are offset according to what is defined in the node.
        """
        # TODO: Fix this so that evaluate_paths is actually used again.
        #       This should also correctly check the period offset type,
        #       and probably should also be a getter/setter

        dep_date = date(year, month, 1) - relativedelta(months=self.period_offset)
        self._hdfs_paths = [
            os.path.join(self.base_path,
                         self.__class__.__name__,
                         f"year={dep_date.year}",
                         f"month={dep_date.month}")
        ]

    def evaluate_paths(self, year, month) -> None:
        """
        Determines the paths on HDFS/local filesystem with respect to the run date
        """

        found_paths = [p[0] for p in list_table_partitions(self.table_path)]
        filtered_paths = list(filter(lambda p: chronological_sorter(p) <= f"{year}.{month:02d}", found_paths))
        chronological_paths = sorted(filtered_paths, key=chronological_sorter)
        neat_order = "\n" + "\n".join(chronological_paths)
        logging.debug(f"Returning paths: {neat_order}")
        self._hdfs_paths = chronological_paths[-(self.total_offset() + 1):]

    def write_table(self, year, month) -> None:
        """
        Writes the dataset to the chosen table store using partitions of year and
        month, as a series of parquet files. Will overwrite any existing partition
        if the node period is static.
        """

        # Ensure a dataframe is returned by the node's generate method
        # and add the generated date to the schema.
        output = self.generate_node(year, month)

        if not output:
            raise Exception(f"Nothing was returned by the node's generate method.")

        output_df = (
            output
            .withColumn("year", f.lit(year))
            .withColumn("month", f.lit(month))
        )

        # Remove any previously-generated files for the month for that node
        clear_table_partition(f"{self.table_path}/year={year}/month={month}")

        output_df.write.parquet(path=self.table_path, partitionBy=["year", "month"], mode="append")


class BaseWideTable(BaseTableNode):
    """
    Overrides the fit_to_schema method of the BaseTableNode,
    for nodes that have very many columns (wide tables). For
    such cases, manually specifying the schema can be more
    pain than it's worth, given such tables change regularly
    over time.

    Since the wide tables are typically made by joining
    dataframes, an id column must be present in the schema.
    """

    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)
        self.id_col = None

    def fit_to_schema(self, dataframe: DataFrame) -> DataFrame:

        try:
            dataframe.select(self.id_col).columns
        except AnalysisException:
            raise Exception(f"You have not specified a an id column for the class: {self.__class__.__name__}")

        return dataframe


class BaseDataSource(BaseTableNode):
    """
    Class for which there is no dependency, and therefore no generate method required.
    """
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)
        self.base_path = self.get_base_path()
        self.dataiku_project_name = ""
        self.dataiku_table_name = ""
        self.table_path = os.path.join(self.base_path, self.__class__.__name__)
        self.dependency_list = []

    def get_base_path(self) -> str:
        """
        Gets the prefix for the file store location
        """
        base_path = self.configuration.settings.get("base_path")
        # Append the bucket and s3 prefix to the path
        if self.configuration.settings.get("aws"):
            bucket = self.configuration.s3.get("bucket")
            return f"s3://{bucket}/{base_path}/data_source/{self.project_name}"
        else:
            return os.path.join("file:", base_path, self.project_name)

    def generate_node(self, year, month) -> DataFrame:
        pass

    def fit_to_schema(self, dataframe) -> DataFrame:
        pass

    def write_table(self, year, month) -> None:
        pass


class ExternalDataSource(BaseDataSource):
    """
    Case where there is a dependency in another bucket or HDFS-valid URI.
    The full path to the table must be specified.
    """

    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)
        self.base_path = self.get_base_path()

    def set_external_dependency(self, table_path: str):
        self.table_path = table_path
        logging.debug(f"Finding external dependency: {table_path}")

    def get_dataframe(self, year, month) -> DataFrame:
        """
        Returns a dataframe for the given dependency over the given period.

        Assumes all source data is in a single partition!

        :return: Spark SQL DataFrame
        """

        if not self._sparkSession:
            self.init_sparkSession()

        found_paths = [p[0] for p in list_table_partitions(self.table_path)]

        if len(found_paths) > 1:
            logging.warning(f"Found external data source {self.table_path} which has multiple partitions!"
                            f"Reading as one single partition.")
        elif len(found_paths) == 0:
            raise FileNotFoundError(f"Cannot find the data source {self.table_path}.")

        return self._sparkSession.read.parquet(*found_paths)
