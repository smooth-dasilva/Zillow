import os
import contextlib
import mysql.connector
from getpass import getpass
import re
from collections import deque

class mysql_conn_class:

    def __init__(self, _app_logger, mysqldb) -> None:
        self.app_logger = _app_logger
        self.dbname = mysqldb

    @contextlib.contextmanager
    def get_mysql_conn(self, db=""):
        if db != "" :
            """
            Context manager used for when specifying database (eg add a record). db kwarg specified
            """
            conn = mysql.connector.connect(host='localhost',
                                        user='root',
                                        password=os.environ.get('MYSQL_PWD'),
                                        database=db)
            try:
                yield conn
            except Exception as e:
                self.app_logger.exception(e)
            finally:
                conn.close()
        else:

            """
            Context manager for when not specifying database (eg when creating database). db kwargs not used
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
                    else: return []
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
        except Exception as e:
            self.app_logger.exception(e)

    def get_tables_in_database(self, dbname):
        #best practices is to use deques instead of lists if possible . 
        #they are just  list equivalent with ensured performant left and right appends

        tb_names_list = []
        try:
            with self.get_mysql_conn(dbname) as connection:
                show_tb = "SHOW TABLES"
                
                with connection.cursor() as cursor:
                    cursor.execute(show_tb)
                    for db in cursor:
                        tb_names_list.append(db[0])
                    if tb_names_list: return (tb_names_list)
                    else: return []
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
        except Exception as e:
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



    def create_table(self, dbname, tbquery):####TODO
        try:
            with self.get_mysql_conn(dbname) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(tbquery)
                    connection.commit()
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
            self.app_logger.error("Error creating new table")

    def gen_sql_type(self, pd_type):
        if pd_type=="Int64": return "INT"
        if pd_type=="Float64": return "FLOAT"
        return "VARCHAR(100)"

    def describe_table_database(self, dbname, tbname):
        
        """
        Returns None, simply prints
        """

        try:
            with self.get_mysql_conn(dbname) as connection:
                describe_table_query = f"DESCRIBE {tbname}"
                with connection.cursor() as cursor:
                    cursor.execute(describe_table_query)
                    # Fetch rows from last executed query
                    result = cursor.fetchall()
                    for row in result:
                        print(row)
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
        except Exception as e:
            self.app_logger.exception(e)