import os
import duckdb
import logging

from nyc.connect import postgres_connection
from psycopg2 import extras

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("logger")

NYC_DIR = os.path.dirname(__file__)
DAGS_DIR = os.path.dirname(NYC_DIR)
AIRFLOW_DIR = os.path.dirname(DAGS_DIR)
DATA_DIR = os.path.join(AIRFLOW_DIR, "data")


def read_sql(file_path):
    with open(file_path, "r") as file:
        return file.read()


def create_table_if_exists(table_name, cursor):
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        );
    """)
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:
        logging.info(f"Table {table_name} not exists.")    
        create_sql_file = read_sql(f"{NYC_DIR}/sql/create_table_{table_name}.sql")
        cursor.execute(create_sql_file)
        logging.info(f"Table {table_name} has been created.")
    else:
        logging.info(f"Table {table_name} exists.")


def load_data(path):
    logging.info(f"Loading data from {path}.")
    raw_table = duckdb.read_json(path)
    sql_bronze = read_sql(f"{NYC_DIR}/sql/bronze_table_service_requests.sql")

    bronze_table = duckdb.sql(sql_bronze)
    values = bronze_table.fetchall()

    return values


def insert_database(values, cursor, page_size=100):
    logging.info("Ingesting data into the database...") 
    sql_insert = read_sql(f"{NYC_DIR}/sql/insert_table_service_requests.sql")
    extras.execute_values(cursor, sql_insert, values, template=None, page_size=page_size)
    logging.info("Ingesting data into the database has been completed.") 
    

def bronze_ingest(**kwargs):
    DATE_REFERENCE = kwargs.get("ds")
    TABLE_NAME = kwargs.get("table_name")
    PATH = f"{DATA_DIR}/service-requests/{DATE_REFERENCE}/*.json"
    
    conn, cursor = postgres_connection()

    create_table_if_exists(TABLE_NAME, cursor)

    values = load_data(PATH)
    
    insert_database(values, cursor)
