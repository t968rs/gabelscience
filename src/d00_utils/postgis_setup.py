# src/d00_utils/postgis_setup.py

from src.d00_utils.loggers import getalogger
from sqlalchemy import create_engine
import psycopg2
import sys


logger = getalogger("postgis_setup")


def create_database(engine, db_name):
    conn = engine.connect()
    conn.execute(f"COMMIT")
    conn.execute(f"CREATE DATABASE {db_name}")
    conn.close()

def enable_postgis(engine, db_name):
    db_engine = create_engine(engine.url.set(database=db_name))
    conn = db_engine.connect()
    # noinspection PyTypeChecker
    conn.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    conn.close()


def test_postgis_connection(engine, db_name):
    conn = engine.connect()
    try:
        conn.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
    except psycopg2.OperationalError as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    conn.close()
    logger.info(f"Connection to '{db_name}' successful.")