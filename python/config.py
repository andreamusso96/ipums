from dotenv import load_dotenv
import os
import psycopg2

load_dotenv(dotenv_path='/Users/andrea/Desktop/PhD/Data/ipums/config.env')

db_config = {
    "dbname": os.getenv('POSTGRES_DB'),
    "host": os.getenv('POSTGRES_HOST'),
    "port": os.getenv('POSTGRES_PORT'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD')
}


def get_db_connection():
    print('Connecting to the PostgreSQL database...')
    con = psycopg2.connect(**db_config)
    return con


if __name__ == '__main__':
    get_db_connection()


