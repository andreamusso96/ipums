from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='/Users/andrea/Desktop/PhD/Data/ipums/config.env')

db_config = {
    "dbname": os.getenv('POSTGRES_DB'),
    "host": os.getenv('POSTGRES_HOST'),
    "port": os.getenv('POSTGRES_PORT'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD')
}


