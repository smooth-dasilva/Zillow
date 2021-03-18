import pandas as pd
import time

from Utilities.logger import setup_logger
from quickstart import SendMessage
from Utilities.mysql_conn import mysql_conn_class
from Experts.FileExpert import FileExpertClass
from Utilities.helpers import *

#Send email of app log to self vars
to = "luan.dasilva@smoothstack.com"
sender = "luan.dasilva@smootstack.com"
subject = "Automated Log File Email"
msgHtml = "Log file from app run"
msgPlain = "Log file from app run"

os.chdir("C:/Users/luanf/OneDrive/Desktop/ZillowMAIN/ZillowEnv")
start = time.time()

app_logger = setup_logger('app_logger', './app.log')
logSeparator = '++++++++++++++++++++++++++++'
app_logger.info(f"Begin app log\n{logSeparator}")

def main():
    DatabaseExpert = mysql_conn_class(app_logger, mysqldb="zillowdb")
    db_names_in_mysql = DatabaseExpert.get_database_names()
    if DatabaseExpert.dbname not in db_names_in_mysql:
        DatabaseExpert.create_database('zillowdb')

    

    fileExpert = FileExpertClass(app_logger, r'[A-Za-z0-9]+_time_series.csv', './datasets/')
    if fileExpert.path=="ERROR":
        return
    if not fileExpert.NameDeq:
        return app_logger.error(f"No files with regex pattern {fileExpert.pattern} match in the given {fileExpert.path}. \nTerminating the program...")

    tb_names_in_db = DatabaseExpert.get_tables_in_database(DatabaseExpert.dbname)
    
    for name in fileExpert.NameDeq:
        tbname = name[:-4].lower()
    
        """
        Create empty staging tables
        """
        fullpath=fileExpert.path+name
        if is_file_empty(fullpath):
            app_logger.error("Found empty file ")
            #send email with source file
    
        elif tbname not in tb_names_in_db: 
            try:
                app_logger.info(f"Processing file: {name}")
                
                df = pd.read_csv(fullpath, low_memory=False)
                df.convert_dtypes()
               # df = replace_nulls_with(data=df, replacewith="NAN", logger=app_logger)
                if not df.empty:
                    #TODO USE config file
                    tarLoc = 'c:/Users/luanf/OneDrive/Desktop/ZillowMAIN/ZillowEnv/datasets/archive/Preprocess-'+name
                    try:
                        df.to_csv('c:/Users/luanf/OneDrive/Desktop/ZillowMAIN/ZillowEnv/datasets/archive/Preprocess-'+name)
                        archive_file(tarLoc, tarLoc+'.tar.gz')
                        SendMessage(sender, to, subject, msgHtml, msgPlain, tarLoc+".tar.gz")
                        ...
                    except Exception as e:
                        app_logger.error(f"Caught an error trying to send {tbname} to csv, compressing and archiving it, or sending the email... ")
                        app_logger.exception(e)

                if (tbname not in tb_names_in_db):

                    tbquery= f'CREATE TABLE {tbname} ('
                    for colname, type in df.dtypes.items():
                        tbquery+=f' {colname} {get_type(type)},'
                    tbquery=tbquery[:-1]+')' #remove extra , and add ) to match sql create query
                    DatabaseExpert.create_table(DatabaseExpert.dbname, tbquery)
            except Exception as e:
                app_logger.error(f"There was a problem when creating the table query for {tbname}... Please examine the following stack trace to determine the issue:")
                app_logger.exception(e)

if __name__ =="__main__":
    main()


# logger ending
app_logger.info(f"End app log\n{logSeparator}")

end = time.time()
print(end - start)