import os
import contextlib
import mysql.connector
from getpass import getpass
import re
from collections import deque

class mysql_conn_class:

    def __init__(self, _app_logger) -> None:
        self.app_logger = _app_logger

    @contextlib.contextmanager
    def get_mysql_conn(self, db=""):
        if db != "" :
            """
            Context manager used for when specifying database (eg add a record). 
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
        else:

            """
            Context manager for when not specifying database (eg when creating table). db kwargs not used
            """

            conn = mysql.connector.connect(host='localhost',
                                        user='root',
                                        password=os.environ.get('MYSQL_PWD'),
                                        )
            try:
                yield conn
            except Exception as e:
                self.app_logger.exception(e)
            finally:
                conn.close()

    def get_database_names(self):
        #best practices is to use deques instead of lists if possible . 
        #they are just  list equivalent with ensured performant left and right appends

        db_names_list = []
        try:
            with self.get_mysql_conn() as connection:
                show_db = "SHOW DATABASES"
                
                with connection.cursor() as cursor:
                    cursor.execute(show_db)
                    for db in cursor:
                        db_names_list.append(db[0])
                    if db_names_list: return (db_names_list)
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
        
    def create_database(self, dbname):
        try:
            with self.get_mysql_conn() as connection:
                create_db = f"CREATE DATABASE {dbname}"
                with connection.cursor() as cursor:
                    cursor.execute(create_db)
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
            self.app_logger.error("Error creating new database")



    def create_table(self, tbname):####TODO
        try:
            with self.get_mysql_conn() as connection:
                create_tb = f"CREATE TABLE {tbname}"
                with connection.cursor() as cursor:
                    cursor.execute(create_tb)
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
            self.app_logger.error("Error creating new table")
