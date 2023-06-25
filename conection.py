import psycopg2
from psycopg2 import pool
from dotenv import dotenv_values
from queue import Queue
from threading import Thread
import pandas as pd
from sqlalchemy import create_engine
import io

config = dotenv_values(".env")


# Define a function to create a PostgreSQL connection
def create_connection():
    try:
        conn = psycopg2.connect(dbname=config['DB'],
            user=config['USER'],
            password=config['PASSWORD'],
            host=config['HOST'],
            port=config['PORT']
        )
        return conn
    except psycopg2.Error as e:
        print(e)


# Define a function to initialize the connection pool
def initialize_pool():
    global connection_pool
    connection_pool = Queue(maxsize=5)
    for _ in range(5):
        connection = create_connection()
        connection_pool.put(connection)


# Define a function to get a database connection from the pool
def get_connection():
    connection = connection_pool.get()
    if connection is None:
        connection = create_connection()
    return connection


# Define a function to release a database connection back to the pool
def release_connection(connection):
    if connection_pool.qsize() < 5:
        connection_pool.put(connection)
    else:
        connection.close()


# Define a function to execute a query using a database connection
def execute_query(query):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    release_connection(connection)
    return result, column_names

# Initialize the connection pool
initialize_pool()


def new_model(df, name_model, name_bd, action='append') -> None:
    """
    function in charge of creating a new model in the database
    from a client.
    """
    url = config['URL_BD']
    engine = create_engine(f'{url}/{name_bd}')


    df.to_sql(name_model, engine, if_exists=action, index=False)