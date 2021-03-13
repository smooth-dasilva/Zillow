import os
import contextlib
import mysql.connector
from getpass import getpass

from collections import deque

class mysql_conn_class:

    def __init__(self, _app_logger) -> None:
        self.app_logger = _app_logger

    @contextlib.contextmanager
    def get_update_mysql_conn(self, db):
        """
        Context manager used for when database is in MySql
        
        """
        conn = mysql.connector.connect(host='localhost',
                                    user='root',
                                    password=getpass("Enter password: "),
                                    database=db)
        try:
            yield conn
        except Exception as e:
            self.app_logger.exception(e)
        finally:
            conn.close()

    @contextlib.contextmanager
    def get_add_mysql_conn(self):
        """
        Context manager for when no database in sql (eg when creating table)
        """

        conn = mysql.connector.connect(host='localhost',
                                    user='root',
                                    password=getpass("Enter password: "),
                                    )
        try:
            yield conn
        except Exception as e:
            self.app_logger.excpetion(e)
        finally:
            conn.close()

    def get_database_names(self):
        #best practices is to use deques instead of lists if possible . 
        #they are just  list equivalent with ensured performant left and right appends

        db_names_deq = deque()
        try:
            with self.get_add_mysql_conn() as connection:
                show_db = "SHOW DATABASES"
                with connection.cursor() as cursor:
                    cursor.execute(show_db)
                    for db in cursor:
                        db_names_deq.append(db)
                    if db_names_deq: return (db_names_deq)
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
