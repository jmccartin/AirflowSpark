import datetime
import logging
from typing import List


def last_day_of_month(any_day: datetime.datetime):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


def most_recent():
    """
    Same as last(), but allows for date future to that of
    the run date.
    Warning! This might allow data temporal leakage!
    """
    raise NotImplementedError


def current() -> int:
    """
    Default choice for most workflows.
    Returns the same month in the dependency as for the generated node.
    """
    return 0


def previous(offset=1) -> int:
    """
    Defaults to the previous period (i.e, one month before).
    """
    return offset


def last_available():
    """
    Gets the last available data on the table store, but
    disallowing dates future to that of the run date.
    """

    return 0


def chronological_sorter(path: str) -> str:
    """
    Returns a formatted year/month string for use in
    chronological sorting of hdfs paths
    """
    year_split = path.split("year=")[-1]
    year = int(year_split.split("/")[0])
    month = int(year_split.split("month=")[-1].split("/")[0])
    return f"{year}.{month:02d}"
