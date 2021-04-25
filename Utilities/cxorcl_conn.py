import contextlib
import cx_Oracle
import pandas as pd

class orcl_conn_class:

    def __init__(self, _app_logger, usr, pwd,  p_host, p_service, p_port) -> None:
        self.app_logger = _app_logger
        self.user = usr
        self.pwd = pwd
        self.p_host = p_host
        self.p_service = p_service
        self.p_port = p_port

    @contextlib.contextmanager
    def get_orcl_conn(self):
            dsn = cx_Oracle.makedsn(host=self.p_host, port=self.p_port, sid=self.p_service)
            conn = cx_Oracle.connect(
                self.user,
                self.pwd,
                dsn=dsn
            )                      
            try:
                yield conn
            except cx_Oracle.DatabaseError as e:
                self.app_logger.exception(e)
            finally:
                conn.close()

    def get_orcl_conn_version(self):
        try:
            with self.get_orcl_conn() as conn:
                print(f"Connection version: {conn.version}")
        except cx_Oracle.DatabaseError as e:
            self.app_logger.exception(e)
    
    def get_sql_query_reult(self, query):
        try:
            with self.get_orcl_conn() as conn:
                return pd.read_sql(query, conn)
        except cx_Oracle.DatabaseError as e:
            self.app_logger.exception(e)

    def create_table(self, colnames, tbname):
        filenamesplit = tbname.split("_")
        fileend = "TS"
        if filenamesplit[-1] == "crosswalk": fileend = "CW"
        try:
            tbname = tbname.replace(" ", "").split("_")[0]
            createTableQuery = f"""CREATE TABLE STG_{filenamesplit[0].replace(" ", "")}_{fileend} ("""
            for colname in colnames:
                createTableQuery += " "+ colname + """ VARCHAR(50),"""
            createTableQuery = createTableQuery[:-1] + """)"""
            with self.get_orcl_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(createTableQuery)
                conn.commit()
        except cx_Oracle.DatabaseError as e:
            self.app_logger.exception(e)
