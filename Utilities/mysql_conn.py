import os
import contextlib
import mysql.connector
from getpass import getpass

@contextlib.contextmanager
def get_mysql_conn(db):
    """
    Context manager to automatically close DB connection. 
    
    """
    conn = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password=getpass("Enter password: "),
                                   database=db)
    try:
        yield conn
    finally:
        conn.close()


def get_mysql_dbs(app_logger):
    try:
        with mysql.connector.connect(
            host="localhost",
            user="root",
            password=getpass("Enter password: "),
        ) as connection:
            show_db = "SHOW DATABASES"
            with connection.cursor() as cursor:
                cursor.execute(show_db)
                for db in cursor:
                    print(db)

    except mysql.connector.Error as e:
        app_logger.exception(e)
