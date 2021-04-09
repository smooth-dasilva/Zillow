import contextlib
import cx_Oracle
import pandas as pd


class orcl_conn_class:

    def __init__(self, usr, p_host, p_service) -> None:
        self.user = usr
        self.p_host = p_host
        self.p_service = p_service
    @contextlib.contextmanager
    def get_orcl_conn(self):
            conn = cx_Oracle.connect(
                self.user,
                'root',
                "{0}/{1}".format(self.p_host, self.p_service)
            )                       
            try:
                yield conn
            except cx_Oracle.DatabaseError as e:
                # error, = e.args
                # if error.code == 1017:
                #     self.app_logger.error("Please check credentials given...")
                print("Tracebac:k\n"+e)
            finally:
                conn.close()
    def get_orcl_conn_version(self):
        try:
            with self.get_orcl_conn() as conn:
                print(f"Connection version: {conn.version}")
        except cx_Oracle.DatabaseError as e:
            print(e)
    def get_sql_query_reult(self, query):
        try:
            with self.get_orcl_conn() as conn:
                return pd.read_sql(query, conn)
        except cx_Oracle.DatabaseError as e:
            print(e)

