from contextlib import contextmanager
from typing import Iterable, Tuple, Any

import pandas as pd
import psycopg2
import psycopg2.extras

from config import config
from queries import create_table, drop_table


@contextmanager
def get_db_cursor():
    """Creates a db connection as a context manager."""
    host = config["postgres"]["host"]
    dbname = config["postgres"]["dbname"]
    user = config["postgres"]["user"]
    password = config["postgres"]["password"]
    conn_str = f"host={host} dbname={dbname} user={user} password={password}"
    conn = psycopg2.connect(conn_str)
    conn.set_session(autocommit=True)

    yield conn.cursor()

    conn.close()


def create_tables():
    """Creates each table using the queries in `create_table.ddl_queries` tuple."""
    for ddl in create_table.ddl_queries:
        with get_db_cursor() as cur:
            cur.execute(ddl)


def drop_tables():
    """Drops each table using the queries in `drop_table.drop_queries` tuple."""
    for drop in drop_table.drop_queries:
        with get_db_cursor() as cur:
            cur.execute(drop)


def reset_database():
    """Resets database to a clean state."""
    drop_tables()
    create_tables()


def insert_data(insert_query: str, data_list: Iterable[Tuple[Any, ...]]):
    """Inserts a list of tuples into the database with the help of the given insert into query."""
    with get_db_cursor() as cur:
        for data_tuple in data_list:
            cur.execute(insert_query, data_tuple)


def bulk_insert_data(insert_query: str, df: pd.DataFrame, page_size: int = 10_000):
    """Bulk inserts a dataframe into the database with the help of the given insert into query.

    See the documentation of the psycopg2 package for details."""
    data = df.itertuples(index=False, name=None)

    with get_db_cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_query, data, template=None, page_size=page_size)
