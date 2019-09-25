import datetime
import logging
from typing import List


def last_day_of_month(any_day: datetime.datetime):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


def last():
    pass

def Daily():
    pass

def Monthly():
    pass

def Static():
    pass


def chronological_sorter(path):
    year_split = path.split("year=")[-1]
    year = int(year_split.split("/")[0])
    month = int(year_split.split("month=")[-1].split("/")[0])
    return f"{year}.{month:02d}"


def evaluate_periods(paths, period_start, period_end, offset) -> List[str]:
    chron_paths = sorted(paths, key=chronological_sorter)
    neat_order = "\n" + "\n".join(chron_paths)
    logging.debug(f"Returning paths: {neat_order}")
    if period_start == last():
        return chron_paths[-(1 + offset):]
