import os
import logging
import psycopg2


db_params = {
    'host': os.environ.get("POSTGRES_HOST"),
    'database': os.environ.get("POSTGRES_DB"),
    'user': os.environ.get("POSTGRES_USER"),
    'password': os.environ.get("POSTGRES_PASSWORD"),
    'port': os.environ.get("POSTGRES_PORT")
}

def postgres_connection():
    try:
        conn = psycopg2.connect(**db_params)
        logging.info("Connection established successfully!")
        conn.autocommit = True
        return conn, conn.cursor()
    
    except Exception as e:
        logging.info(f"Error connecting to PostgreSQL database: {e}")