import psycopg2
from dotenv import dotenv_values

config = dotenv_values(".env")

conn = psycopg2.connect(dbname=config['DB'],
    user=config['USER'],
    password=config['PASSWORD'],
    host=config['HOST'],
    port=config['PORT']
)