import pytest

from src.main.python.time_utils import evaluate_periods

def test_evaluate_periods():

    test_paths = [
        "/path/to/dataset/year=2019/month=12",
        "/path/to/dataset/year=2019/month=8",
        "/path/to/dataset/year=2019/month=9",
        "/path/to/dataset/year=2019/month=10",
    ]

    sorted_paths = evaluate_periods()