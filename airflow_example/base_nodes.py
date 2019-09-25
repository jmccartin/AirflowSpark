from abc import ABC, abstractmethod, abstractproperty
import inspect
import glob
import logging
import os
import shutil
from typing import Any, List

from pyspark.sql import DataFrame, functions as f
from pyspark.sql.types import StructType, StructField, IntegerType

from airflow_example.configuration import Configuration
from airflow_example.time_utils import evaluate_periods, Static

class AbstractNode(ABC):
    @abstractmethod
    def generate_node(self, year, month) -> DataFrame:
        pass


class BaseTableNode(AbstractNode):
    def __init__(self, sparkSession):
        self._schema = None
        self._hdfs_paths = None
        self.configuration = Configuration("config/configuration.yml")
        self.dependency = []
        self.base_path = self.configuration.settings.get("base_path")
        self.table_path = os.path.join(self.base_path, self.__class__.__name__)
        self.period = None
        self.sparkSession = sparkSession

    @classmethod
    def get_project_name(self, cls) -> str:
        """
        Sets the name of the Airflow DAG to be the parent directory
        of the initialised table class
        """
        return inspect.getfile(cls.__class__).split("/")[-2]

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

    # def generate_node(self, year, month):
    #     pass

    def fit_to_schema(self, dataframe: DataFrame) -> DataFrame:
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


    def get_dataframe(self) -> DataFrame:
        """
        Returns a dataframe for the given dependency over the given period.
        :return: Spark SQL DataFrame
        """

        dependency_df = self.sparkSession.read.parquet(str(*self._hdfs_paths))
        self.verify_schema(dependency_df)

        return dependency_df

    def set_period(self, period_start, period_end=None, offset=0):
        if not os.path.exists(self.base_path):
            raise FileNotFoundError(f"The directory {self.base_path} does not exist")
        all_paths = glob.glob(os.path.join(self.table_path, "*", "*"))
        self._hdfs_paths = evaluate_periods(all_paths, period_start, period_end, offset)
        return self

    def write_table(self, year, month) -> None:
        output_df = (
            self.generate_node(year, month)
            .withColumn("year", f.lit(year))
            .withColumn("month", f.lit(month))
        )

        if self.period == Static():
            write_mode = "overwrite"
        else:
            write_mode = "append"

        # Remove any previously-generated files for the month for that node
        node_current = os.path.join(self.table_path, f"year={year}", f"month={month}")
        if os.path.exists(node_current):
            logging.warning(f"Found previously generated data for year={year}, month={month}. Removing files.")
            shutil.rmtree(node_current)

        output_df.write.parquet(path=self.table_path, partitionBy=["year", "month"], mode=write_mode)


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
