import os
import contextlib
import mysql
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

import config

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
            conn = mysql.connector.connect(host=config.orcl_host,
                                        user=config.orcl_user,
                                        password=os.environ.get('MYSQL_PWD'),
                                        database=db)
            try:
                yield conn
            except mysql.connector.Error as e:
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

    def create_table(self, dbname, tbquery):
        try:
            with self.get_mysql_conn(dbname) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(tbquery)
                    connection.commit()
        except mysql.connector.Error as e:
            self.app_logger.exception(e)
            self.app_logger.error("Error creating new table")

    def add_dataframe_to_db(self, dbname, tbname,df):####TODO Better
        try:
            hostname="localhost"
            uname="root"
            pwd = os.environ.get('MYSQL_PWD')
            engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                            .format(host=hostname, db=dbname, user=uname, pw=pwd))
            database_username = 'root'
            database_password = os.environ.get('MYSQL_PWD')
            database_ip       = '127.0.0.1'
            database_name     = dbname
            database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                        format(database_username, database_password, 
                                                                database_ip, database_name), pool_recycle=1, pool_timeout=57600)

            df.to_sql(con=engine, name=tbname, if_exists='replace',chunksize=100)
        except Exception as e:
            self.app_logger.exception(e)
            self.app_logger.error(f"Error adding dataframe to {tbname} in datafram {dbname}")

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

    def gen_sql_type(pd_type):
        if pd_type=="Int64": return "INT"
        if pd_type=="Float64": return "FLOAT"
        return "VARCHAR(100)"