import logging
import psycopg2
import pandas as pd

from python.config import db_config


# Create a logger
def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def execute_sql(query: str, con=None):
    con = get_db_connection() if con is None else con
    with con.cursor() as cursor:
        cursor.execute(query)
        con.commit()
    con.close()


def sql_to_pandas(query: str, con=None):
    con = get_db_connection() if con is None else con
    df = pd.read_sql(query, con=con)
    con.close()
    return df


def get_db_connection():
    con = psycopg2.connect(**db_config)
    return con


def next_census_year(year: int) -> int:
    assert 1850 <= year <= 1930, f"Year must be between 1850 and 1940, but got {year}"
    if year == 1880:
        return 1900
    else:
        return year + 10


def previous_census_year(year: int) -> int:
    assert 1860 <= year <= 1940, f"Year must be between 1850 and 1940, but got {year}"
    if year == 1900:
        return 1880
    else:
        return year - 10