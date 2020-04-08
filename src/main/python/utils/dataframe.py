# PySpark
import pyspark
from pyspark.sql.types import *

# Python
import logging
import numpy as np
import pandas as pd
from typing import Any, Dict


def assert_test_dfs_equal(expected_df: pyspark.sql.DataFrame,
                          generated_df: pyspark.sql.DataFrame) -> None:
    """
    Used to compare two dataframes (typically, in a unit test).
    Better than the direct df1.equals(df2) method, as this function
    allows for tolerances in the floating point columns, and is
    also more descriptive with which parts of the two dataframes
    are in disagreement.
    :param expected_df: First dataframe to compare
    :param generated_df: Second dataframe to compare
    """

    row_limit = 10000

    e_count = expected_df.count()
    g_count = generated_df.count()

    if (e_count > row_limit) or (g_count > row_limit):
        raise Exception(f"One or both of the dataframes passed has too many rows (>{row_limit})."
                        f"Please limit your test sizes to be lower than this number.")

    assert e_count == g_count, "The dataframes have a different number of rows."

    expected_pdf = expected_df.toPandas()
    generated_pdf = generated_df.toPandas()

    assert list(expected_pdf.columns) == list(generated_pdf.columns), \
        "The two dataframes have different columns."

    for col in expected_pdf.columns:
        error_msg = f"The columns with name: `{col}` were not equal."
        if expected_pdf[col].dtype.type == np.object_:
            assert expected_pdf[[col]].equals(generated_pdf[[col]]), error_msg
        else:
            # Numpy will not equate nulls on both sides. Filter them out.
            expected_pdf = expected_pdf[expected_pdf[col].notnull()]
            generated_pdf = generated_pdf[generated_pdf[col].notnull()]
            try:
                is_close = np.allclose(expected_pdf[col].values, generated_pdf[col].values)
            except ValueError:
                logging.error(f"Problem encountered while equating column '{col}'.")
                raise
            assert is_close, error_msg


def dict_to_pandas(value_dict: Dict[str, Dict[str, Any]], first_column: str) -> pd.DataFrame:
    """
    Converts a dictionary (typically a config YAML) into a dataframe.
    Keys/values must be fully represented, otherwise conversion will fail.
    :param value_dict: Dictionary requiring conversion
    :param first_column: Name of the top level key in the dictionary that will form the first column.
    :return: Pandas Dataframe with all of the fields and values
    """
    row_data = []
    header = [first_column]

    for column in value_dict.keys():
        row = [column]
        for field, value in value_dict.get(column).items():
            if field not in header:
                header.append(field)
            row.append(value)
        row_data.append(tuple(row))

    return pd.DataFrame(data=row_data, columns=header)


def get_spark_type(data_type: str) -> pyspark.sql.types:
    """
    Gets the imported Spark data type from globals() (therefore must be already imported).
    Raises an exception if there is a typo in the name.
    :param data_type: Exact name of the data type
    :return: Spark data type (object)
    """
    try:
        # TODO: Is there a better way than loading all spark types to globals?
        spark_type = globals()[data_type]()
    except KeyError:
        raise Exception(f"Could not find the Spark data type: {data_type}")
    return spark_type
