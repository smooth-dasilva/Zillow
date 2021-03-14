import pandas as pd
import re

from Utilities.logger import setup_logger
from Utilities.quickstart import SendMessage
from Utilities.mysql_conn import mysql_conn_class
from Experts.FileExpert import FileExpertClass


import time



#start timer
start = time.time()



# set up loggers
app_logger = setup_logger('app_logger', './app.log')

# for log session separator
logSeparator = '++++++++++++++++++++++++++++'

# On app load : create logs
app_logger.info(f"Begin app log\n{logSeparator}")







def main():

    DatabaseExpert = mysql_conn_class(app_logger, mysqldb="zillowdb")
    db_names_in_mysql = DatabaseExpert.get_database_names()
    
    
    #create staging area for dataset pre etl
    if DatabaseExpert.dbname not in db_names_in_mysql:
        DatabaseExpert.create_database('zillowdb')

    #we use a file expert to get all the right dataset names into a list that we access along with path (fileExpert.NameDeq+path)
    fileExpert = FileExpertClass(app_logger, r'[A-Za-z0-9]+_time_series.csv', './datasets/')
    
    #we set FE's path to be ERROR if Check_Validity doesnt get a valid path
    if fileExpert.path=="ERROR":
        return

    if not fileExpert.NameDeq:
        return app_logger.error(f"No files with regex pattern {fileExpert.pattern} match in the given {fileExpert.path}. \nTerminating the program...")

    """
    We have: 
    Validation for directory and filename
    List of all dataset path +  file names  (and so access to pandas df or spark df)
    Logger that can record anything anywhere
    Access to mysql database. Created actual zillowdb
    though we dont have any tables thats nothing but a sql query away
    Able to tar and archive (not implemented yet)
    """
    
    """
    Creates table in staging db for each csv if not exists.
    """
    for name in fileExpert.NameDeq:
        # here we will process the each files in a different loop
        #for now process 1 specified
        
        tb_names_in_db = DatabaseExpert.get_tables_in_database(DatabaseExpert.dbname)
        tbname = name[:-4].lower()
        DatabaseExpert.describe_table_database(DatabaseExpert.dbname, "neighborhood_time_series" )
        if tbname not in tb_names_in_db: 
            df = pd.read_csv(fileExpert.path+name, low_memory=False)
            col_types = (df.convert_dtypes().dtypes)
            """
            TODO : 
            Fill na
            Add to table
            """


            
            try:
                tbquery= f'CREATE TABLE {tbname} ('
                for colname, type in col_types.items():
                    tbquery+=f' {colname} {DatabaseExpert.gen_sql_type(type)},'
                #remove extra , and add )

                tbquery=tbquery[:-1]+')'
                if tbname not in tb_names_in_db: 
                    DatabaseExpert.create_table(DatabaseExpert.dbname, tbquery)
                    
                

            

                
            except Exception as e:
                app_logger.error("There was a problem when creating the table query... Please examine the following stack trace to determine the issue:")
                return app_logger.exception(e)




    #after the above loop, ideally we'll have all the files processed and added to the database. 
    #this may need to altered depending on that complexity...maybe not added to db just tables created



if __name__ =="__main__":
    main()


# logger ending
app_logger.info(f"End app log\n{logSeparator}")

end = time.time()
print(end - start)