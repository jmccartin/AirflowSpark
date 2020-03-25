import pandas as pd

from src.main.python.utils.dataframe import dict_to_pandas


def test_dict_to_pandas():

    data = {
        "entry1": {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        },
        "entry2": {
            "key1": "value4",
            "key2": "value5",
            "key3": "value6"
        }
    }

    generated_df = dict_to_pandas(data, first_column="entries")

    data_tuple = [
        ("entry1", "value1", "value2", "value3"),
        ("entry2", "value4", "value5", "value6"),
    ]

    expected_df = pd.DataFrame(data=data_tuple, columns=["entries", "key1", "key2", "key3"])

    assert expected_df.equals(generated_df), "The dataframes were not equal."
