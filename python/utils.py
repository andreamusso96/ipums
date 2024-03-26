import logging
import psycopg2
from config import db_config


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


def get_db_connection():
    print('Connecting to the PostgreSQL database...')
    con = psycopg2.connect(**db_config)
    return con